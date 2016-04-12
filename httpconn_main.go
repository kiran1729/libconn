// Copyright 2014 ZeroStack, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// HTTPConn is HTTP messaging implemented on top of a TCP socket provided by
// the user. Both ends of the TCP are explicitly handling the messages rather
// than trying to hand the socket to a ListenAndServe loop.
// The internals of the library are designed to handle duplex connections.
// Currently, only simplex operation is supported. If the connection has an
// error, the user will receive an error on the supplied errCh. It is the
// user responsibility to update the connection by calling ResetConn with a
// new connection.
//
// Example (see test for all steps):
//   // Create Connection
//   conn := net.Dial("tcp", "server:port")
//
//   // CLIENT: create channels for sending to and receiving from HTTPConn
//   send := make(chan *HTTPSendReq, size)
//   sendRsp := make(chan *HTTPSendRsp, size)
//   // Create HTTPConn
//   url := "http://server:port/path"
//   timeout := 10 * time.Second
//   maxPipeReqs := 5
//   httpConn := NewHTTPConnClient(timeout, maxPipeReqs, url, send, sendRsp,
//     errCh, 0, 0)
//   send <- &HTTPSendReq{Req: []byte{"test"}}
//   rsp <- sendRsp
//
//   // SERVER: create channels for communication
//   rcvd := make(chan *HTTPRcvdReq, size)
//   rcvdRsp := make(chan *HTTPRcvdRsp, size)
//   // Create HTTPConn
//   url := "http://server:port/path"
//   timeout := 10
//   maxPipeReqs := 5
//   HTTPConn := NewHTTPConnServer(timeout, maxPipeReqs, url, rcvd, rcvdRsp,
//     errCh, 0, 0)
//   msg := <- rcvd
//   rsp := HTTPRcvdRsp{Req: msg, Rsp: nil}
//   rcvdRsp <- rsp
//
////////////////////////////////////////////////////////////////////////////////
// Near Term:
// TODO - make it a reader/writer instead of net.Conn.
//
// TODO(kiran):
// - Handle http proxies.
// - Do duplex connection?
// - Do we need to distinguish between different incarnations of
//   same client to server connections?
// - Would we ever want per request response channels rather than
//   same response channel for all requests?
// - Do we want to make forcing of ordering of responses an option?
// - Make errors on rsp richer than nil/non-nil?

package libconn

import (
  "bufio"
  "crypto/tls"
  "encoding/base64"
  "errors"
  "fmt"
  "io"
  "net"
  "net/http"
  "net/url"
  "strings"
  "sync"
  "time"

  "github.com/golang/glog"

  "zerostack/common/libsec"
  "zerostack/common/util"
)

const (
  gRPCHeader      string        = "X-ZS-Service-Method"
  gErrorHeader    string        = "X-ZS-Return-Status"
  cDefaultChSize  int           = 10
  cTimeoutDur     time.Duration = 300 * time.Second
  cMaxPipeReqs    int           = 10
  cReqChSize      int           = 10
  cRspChSize      int           = 10
  cMaxPendingReqs int           = 100
)

// ErrInit is an uninitialized connection.
var ErrInit = errors.New("uninitialized connection state")

// ErrClient is a client side error where the rpc has not been sent.
var ErrClient = errors.New("rpc client error")

// ErrTimeout is the canonical timeout error.
var ErrTimeout = errors.New("request timeout error")

// ErrDisconnect is the canonical disconnect error.
var ErrDisconnect = errors.New("server disconnected error")

// ErrKeepAlive is when connection fails liveness check.
var ErrKeepAlive = errors.New("connection keepalive failed")

// IsConnectionError helps in knowing if this error originated from the
// library versus end-to-end (like in RPC calls)
func IsConnectionError(err error) bool {
  return err == ErrInit || err == ErrTimeout || err == ErrDisconnect ||
    err == ErrClient
}

// sequenceID is sent in the channel and it is a monotonically increasing
// number used for tracking the messages using the ZSSeqIDHeader.
type sequenceID struct {
  id int64
  mu sync.Mutex
}

// Incr increments the sequenceID by one and returns the new value.
func (s *sequenceID) Incr() int64 {
  s.mu.Lock()
  defer s.mu.Unlock()
  s.id++
  return s.id
}

// Get returns the value of the sequenceID
func (s *sequenceID) Get() int64 {
  s.mu.Lock()
  defer s.mu.Unlock()
  return s.id
}

// SetIfGT sets the sequenceID to the input param only if the input is
// greater than the current value of the sequenceID.
func (s *sequenceID) SetIfGT(val int64) error {
  var err error
  s.mu.Lock()
  defer s.mu.Unlock()
  if val > s.id {
    s.id = val
    err = nil
  } else {
    err = fmt.Errorf("new id %v is less than last %v", val, s.id)
  }
  return err
}

// httpClient is the client state of HTTPConn. It is used to store state of
// all requests when HTTPConn is instantiated as a client.
// sendReqCh - the chan HTTPConn waits for requests to send on the connection
// sendRspCh - the chan on which responses to requests on send are pushed
// sentSeqID - sequence id used on the sender side(client) to send
//             monotonically increasing sequenceID
// sentReqMap - list of sent requests that are being tracked to match up
//              responses or time them out and return errors.
// pendReqMap - list of requests not yet sent from the client waiting for
//              connection to be available
// sentReqTmr - timer that is running on the request sent with least
//              deadline. This is reset whenever the sent queue changes.
// pendReqTmr - timer that is running on the request sitting in pending
//              with least deadline. This is reset whenever the sent queue
//              changes.
// sentReqTmrCh - channel on which the sentReqTmr sends the id when the timer
//                  expires
// pendReqTmrCh - channel on which the pendReqTmr sends the id when the timer
//                  expires
// NOTE: All the above structures are similarly duplicated for handling RPCs.
type httpClient struct {
  *rpcClientService

  conn   net.Conn
  writer io.Writer
  connMu sync.Mutex

  sentSeqID    sequenceID
  pendSeqID    sequenceID
  sentReqMap   *util.SyncMap // map[int64]*sendReqState
  pendReqMap   *util.SyncMap // map[int64]*sendReqState
  sentReqTmr   *util.LoadedTimer
  pendReqTmr   *util.LoadedTimer
  sentReqTmrCh chan interface{}
  pendReqTmrCh chan interface{}
  sendReqCh    chan *HTTPSendReq
  sendRspCh    chan *HTTPSendRsp
  // RPC related
  rpcSeqID     sequenceID
  sendRPCCh    chan *sendRPCState
  sentRPCMap   *util.SyncMap // map[int64]*sendRPCState
  pendRPCMap   *util.SyncMap // map[int64]*sendRPCState
  sentRPCTmr   *util.LoadedTimer
  pendRPCTmr   *util.LoadedTimer
  sentRPCTmrCh chan interface{}
  pendRPCTmrCh chan interface{}

  lastSendTime time.Time
  lastRcvdTime time.Time
  keepAliveTmr *util.RestartTimer

  connErrCh chan error
  quitCh    chan struct{}
  stopCh    chan struct{}
}

// An instance of httpServer is created for each accepted connection. The
// main rcvd channels are copied from the HTTPConn structure into the server
// instance so one channel can be used to receive the messages from all
// accepted connections. We also support ServeConn() function which can
// accept specific channels for an already accepted connection to be served
// by libconn.
//
// rcvdReqCh - the chan caller should monitor to look for requests from the
//               other end of the connection
// rcvdRspCh - chan on which caller should send responses back to other end of
//               connection
// rcvdReqMap - what is received from connection and sent to server code for
//              which server responses are yet to be received.
// connErrCh - chan on which connection errors are sent
// rpcServer   - rpc server object on which the received rpc calls are made.
// rcvdSeqID - last received sequenceID that is forwarded to the server.
//   (TODO(kiran): need to account for last acked(rcvRsp) seqID vs rcvd seqID).
type httpServer struct {
  handlerService    *httpHandlerService
  rpcServer         *rpcServerService
  url               *url.URL
  conn              net.Conn
  connMu            sync.Mutex
  stopCh            chan struct{}
  rcvdReqCh         chan *HTTPRcvdReq
  rcvdRspCh         chan *HTTPRcvdRsp
  rcvdSeqID         sequenceID
  rcvdReqMap        *util.SyncMap // map[int64]*http.Request
  rcvdRPCMap        *util.SyncMap // map[int64]*rcvdRPCState
  connErrCh         chan error
  lastDataTime      time.Time
  keepAliveInterval time.Duration
  keepAliveTimeout  time.Duration
  keepAliveTmr      *util.RestartTimer
  wg                sync.WaitGroup
}

// HTTPConn contains the config and state for the http connection over conn.
// isClient - whether we are operating in client mode or server mode.
// HTTPConn embeds httpClient and uses all the methods when it is setup as a
// client. However, it does not embed an httpServer since a server state is
// created for each accepted client connection. For each accepted client
// connection an httpServer is instantiated and the state copied
// over from HTTPConn to httpServer and the server is started. rpcServerService
// is also embedded by HTTPConn since rpcServerService does not have any
// state per connection.
//
// maxPipeReqs - maximum number of HTTP 1.1 pipelined requests to send on the
//               connection. Setting it to 1 means no pipelining.
// url     - only matters for the server when it parses the request and not for
//           HTTPConn since the request is going on already open Conn.
//
// In a simplex connection, client only passes sendReqCh, sendRspCh,
// connErrCh and server only passes rcvdReqCh, rcvdRspCh and connErrCh.
//
type HTTPConn struct {
  *httpClient
  *rpcServerService
  mu             sync.Mutex
  listener       net.Listener // server
  defaultHandler *http.ServeMux

  url *url.URL
  // these are stored in HTTPConn so they persist across Close() and Connect()
  proxyURL          *url.URL
  proxyAuthHeader   string
  timeout           time.Duration
  maxPipeReqs       int
  keepAliveInterval time.Duration
  keepAliveTimeout  time.Duration

  isClient          bool
  connectRetryCount int
  maxRetry          int
  // values initialized in New and passed through to initClientState() or
  // serveLoop().
  sendReqCh    chan *HTTPSendReq
  sendRspCh    chan *HTTPSendRsp
  rcvdReqCh    chan *HTTPRcvdReq
  rcvdRspCh    chan *HTTPRcvdRsp
  connErrCh    chan error
  serverQuitCh chan struct{}
  wg           sync.WaitGroup
}

// NewHTTPConnClient/NewHTTPConnServer creates a new HTTP protocol connection
// using the net.Conn as the transport for the messages. The caller supplies
// the various channels on which the messages and responses are sent and
// received from HTTPConn to the caller and the other end of the net.Conn.
// As of now the library only operates either as a client or a server, but the
// implementation and the httpParser library can work as a duplex connection
// easily.

// timeoutDur - max timeout for requests with a specified deadline
// maxPipelineReqs - specifies how many requests to pipeline. It is only
//           used on client side. (TODO(kiran): handshake with server?)
// url     - url that should be used for the messages on the channel.
//           All messages are sent with same URL.
// sendReq - chan HTTPConn waits for requests to send on the connection
// sendRsp - chan on which responses to requests on send are pushed
// rcvdReq - chan caller should monitor to look for requests from the other
//           end of the connection
// rcvdRsp - chan on which caller should send responses back to other end of
//           connection
// errCh   - chan on which caller will get any errors from parsing or
//           connection.
// keepAliveInterval - specifies keepAlive packets interval. 0 disables it.
// keepAliveTimeout  - specifies timeout for keepAlive is no data on conn.

func newHTTPConn(timeoutDur time.Duration, maxPipelineReqs int, url *url.URL,
  isClient bool, sendReq chan *HTTPSendReq, sendRsp chan *HTTPSendRsp,
  rcvdReq chan *HTTPRcvdReq, rcvdRsp chan *HTTPRcvdRsp, errCh chan error,
  keepAliveInterval, keepAliveTimeout time.Duration,
) (*HTTPConn, error) {

  h := &HTTPConn{timeout: timeoutDur,
    maxPipeReqs: maxPipelineReqs, url: url, isClient: isClient,
    sendReqCh: sendReq, sendRspCh: sendRsp, connErrCh: errCh,
    rcvdReqCh: rcvdReq, rcvdRspCh: rcvdRsp,
    keepAliveInterval: keepAliveInterval, keepAliveTimeout: keepAliveTimeout,
    serverQuitCh: make(chan struct{}),
  }
  if h == nil {
    return nil, fmt.Errorf("could not create HTTPConn")
  }
  if !isClient {
    h.defaultHandler = http.NewServeMux()
    // TODO(kiran): We can also update Serve signature to accept rpc handler
    // or use this default rpc handler just like we do for http handler.
    var err error
    h.rpcServerService, err = newRPCServerService()
    if err != nil {
      return nil, err
    }
  }
  return h, nil
}

// NewHTTPConnClient implicitly programs the connection as a client connection.
func NewHTTPConnClient(urlStr string, timeoutDur time.Duration,
  maxPipelineReqs int, sendReq chan *HTTPSendReq, sendRsp chan *HTTPSendRsp,
  errCh chan error, keepAliveInterval, keepAliveTimeout time.Duration,
) (*HTTPConn, error) {

  // TODO(kiran): This should be fixed in caller
  if !strings.HasPrefix(urlStr, "tcp") && !strings.HasPrefix(urlStr, "http") {
    urlStr = "http://" + urlStr
  }
  url, err := url.Parse(urlStr)
  if err != nil || url == nil || url.Host == "" {
    return nil, fmt.Errorf("invalid address: %s :: %v", urlStr, err)
  }
  return newHTTPConn(timeoutDur, maxPipelineReqs, url, true, sendReq, sendRsp,
    nil, nil, errCh, keepAliveInterval, keepAliveTimeout)
}

// NewHTTPConnServer implicitly programs the connection as a server connection.
func NewHTTPConnServer(urlStr string, rcvdReq chan *HTTPRcvdReq,
  rcvdRsp chan *HTTPRcvdRsp, errCh chan error,
  keepAliveInterval, keepAliveTimeout time.Duration) (*HTTPConn, error) {

  // TODO(kiran): This should be fixed in caller
  if !strings.HasPrefix(urlStr, "tcp") && !strings.HasPrefix(urlStr, "http") {
    urlStr = "http://" + urlStr
  }
  url, err := url.Parse(urlStr)
  if err != nil || url == nil || url.Host == "" {
    return nil, fmt.Errorf("invalid address: %s :: %v", urlStr, err)
  }
  // If user did not bother creating a rsp channel, we make one and embed it
  // in each request.
  if rcvdReq != nil && rcvdRsp == nil {
    rcvdRsp = make(chan *HTTPRcvdRsp, cDefaultChSize)
  }

  // For server timeout has no meaning.
  return newHTTPConn(0, 1, url, false, nil, nil, rcvdReq, rcvdRsp, errCh,
    keepAliveInterval, keepAliveTimeout)
}

// NewServer provides a much simpler interface for HTTPConn (Server) for
// doing rpc calls on the interface without doing any message traffic on same
// interface.
func NewServer(url string) (*HTTPConn, error) {
  if url == "" {
    return nil, fmt.Errorf("empty server url")
  }
  // TODO(kiran): check default params
  return NewHTTPConnServer(url, nil, nil, nil, 0, 0)
}

// NewClient provides a much simpler interface for HTTPConn for
// doing RPC calls if there is no need to send messages on the same connection.
// For fully flexible connection that can send RPCs and protobufs on same
// underlying transport use the NewHTTPConnClient above.
func NewClient(url string) (*HTTPConn, error) {
  return NewClientWithTimeout(url, cTimeoutDur)
}

// NewClientWithTimeout creates a HTTPConn for doing RPC calls.
// "timeoutDur" specifies the connection timeout
func NewClientWithTimeout(url string, timeoutDur time.Duration) (*HTTPConn,
  error) {

  return NewClientWithTimeoutKeepAlive(url, timeoutDur, 0, 0)
}

// NewClientWithTimeoutKeepAlive creates a HTTPConn for doing RPC calls.
// with the specified connection "timeoutDur", "keepAliveInterval" and
// "keepAliveTimeout".
func NewClientWithTimeoutKeepAlive(url string, timeoutDur, keepAliveInterval,
  keepAliveTimeout time.Duration) (*HTTPConn, error) {

  if url == "" {
    return nil, fmt.Errorf("empty server url")
  }
  errCh := make(chan error, 1)
  httpConn, err := NewHTTPConnClient(url, timeoutDur, cMaxPipeReqs,
    nil, nil, errCh, keepAliveInterval, keepAliveTimeout)

  if err != nil || httpConn == nil {
    return nil, err
  }
  err = httpConn.Connect()
  if err != nil {
    return nil, err
  }
  return httpConn, nil
}

// ResetURL will update the url used in the messages.
func (h *HTTPConn) ResetURL(url *url.URL) error {
  glog.V(3).Infof("setting url to %s host %s", url, url.Host)
  h.url = url
  return nil
}

// URL will return the configured URL.
func (h *HTTPConn) URL() *url.URL {
  return h.url
}

////////////////////////////////////////////////////////////////////////////////
//   Client
////////////////////////////////////////////////////////////////////////////////

// SetProxy sets the proxy information for all the client connections.
func (h *HTTPConn) SetProxy(url *url.URL) error {
  h.proxyURL = url
  return nil
}

// Connect establishes a TCP client connection to server for outbound
// communication.
func (h *HTTPConn) Connect() error {
  h.mu.Lock()
  defer h.mu.Unlock()

  // Close any current active connections that the client might not have
  // called Close() on since each connection is supposed to support only one
  // connection at a time and the previous connection state needs to be
  // cleaned up properly.
  if h.httpClient != nil {
    h.closeClientLocked()
  }

  conn, err := h.connectCommon(nil)
  if err != nil || conn == nil {
    return err
  }

  glog.V(1).Infof("client connected to %v address %v\n",
    h.url, conn.RemoteAddr())

  // Initialize some state
  err = h.initClientState()
  if err != nil {
    glog.Errorf("initClientState failed :: %v", err)
    conn.Close()
    return err
  }
  // This is different for standard and tls connections.
  h.httpClient.conn = conn
  h.httpClient.writer = io.Writer(h.httpClient.conn)

  // Start the service loop
  h.wg.Add(1)
  go func() {
    h.startClient(h.httpClient.quitCh)
    h.wg.Done()
  }()
  return nil
}

// ConnectTLS establishes a TLS client connection to server for outbound
// communication.
func (h *HTTPConn) ConnectTLS(config *tls.Config) error {
  h.mu.Lock()
  defer h.mu.Unlock()

  // Close any current active connections that the client might not have
  // called Close() on since each connection is supposed to support only one
  // connection at a time and the previous connection state needs to be
  // cleaned up properly.
  if h.httpClient != nil {
    h.closeClientLocked()
  }

  conn, err := h.connectCommon(config)
  if err != nil || conn == nil {
    return err
  }
  // Connect to tls
  conn = tls.Client(conn, config)
  if conn == nil {
    glog.Errorf("tls.Client returned nil for connection")
    return fmt.Errorf("tls.Client returned nil for connection")
  }
  // Handshake with TLS to get cert
  err = conn.(*tls.Conn).Handshake()
  if err != nil {
    glog.Errorf("certificate check failed for %v - %v\n", h.url, err)
    return err
  }

  glog.V(1).Infof("client connected via TLS to %v remote address %v\n",
    h.url, conn.RemoteAddr())

  // Initialize some state
  err = h.initClientState()
  if err != nil {
    glog.Errorf("initClientState failed :: %v", err)
    conn.Close()
    return err
  }
  // Use the tls client after tls.Client so the Write uses tls.Write. Note
  // that we do not cast it to net.Conn and lose the tls encryption writing
  // capabilities.
  h.httpClient.conn = conn
  h.httpClient.writer = io.Writer(h.httpClient.conn)

  // Start the service loop
  h.wg.Add(1)
  go func() {
    h.startClient(h.httpClient.quitCh)
    h.wg.Done()
  }()
  return nil
}

// initClientState resets the client state for every new connection made
// (including reconnection).
func (h *HTTPConn) initClientState() error {

  h.httpClient = &httpClient{
    sendReqCh:    h.sendReqCh,
    sendRspCh:    h.sendRspCh,
    connErrCh:    h.connErrCh,
    quitCh:       make(chan struct{}),
    stopCh:       make(chan struct{}),
    sentReqMap:   util.NewSyncMap(),
    pendReqMap:   util.NewSyncMap(),
    sentRPCMap:   util.NewSyncMap(),
    pendRPCMap:   util.NewSyncMap(),
    sentReqTmrCh: make(chan interface{}),
    pendReqTmrCh: make(chan interface{}),
    sentRPCTmrCh: make(chan interface{}),
    pendRPCTmrCh: make(chan interface{}),
  }
  // Start pending ids arbitrarily higher to make it easy to debug.
  h.httpClient.pendSeqID.SetIfGT(10000)

  var err error
  h.rpcClientService, err = newRPCClientService()
  if err != nil || h.rpcClientService == nil {
    glog.Error("could not create rpc client: ", err)
    return err
  }
  h.rpcClientService.stopCh = h.httpClient.stopCh
  return nil
}

// connectCommon establishes a TCP client connection to server for outbound
// communication. For TLS connections is uses the provided tls config.
// For proxies, the connection is to proxy. For tls connections via proxy,
// we do the CONNECT to proxy in this function. The basic auth required for
// proxy (specified in the http://user:pass@proxyaddress) is setup here.
// TODO(kiran): NTLM etc. auth is not supported.
func (h *HTTPConn) connectCommon(config *tls.Config) (net.Conn, error) {
  // If message timeout is given, we can at least wait for same time for
  // connection timeout.
  var (
    err       error
    tempDelay time.Duration // how long to sleep on accept failure
    host      string
    conn      net.Conn
  )

  if h.proxyURL != nil {
    host = h.proxyURL.Host
    glog.V(2).Infof("connection to host %s (proxy %s) for target url %s",
      host, h.proxyURL, h.url)
  } else {
    host = h.url.Host
    glog.V(2).Infof("connection to %s for target url %s", host, h.url)
  }

  for {
    conn, err = net.DialTimeout("tcp", host, h.timeout)
    if err != nil || conn == nil {
      if ne, ok := err.(net.Error); ok && ne.Temporary() &&
        h.connectRetryCount <= h.maxRetry {

        // Up the connect retry count even on temporary errors.
        h.connectRetryCount++
        if tempDelay == 0 {
          tempDelay = 5 * time.Millisecond
        } else {
          tempDelay *= 2
        }
        if max := 1 * time.Second; tempDelay > max {
          tempDelay = max
        }
        glog.V(1).Infof("connect error: %v; retrying in %v", err, tempDelay)
        time.Sleep(tempDelay)
        continue
      }
      glog.V(3).Infof("could not connect to host %s :: %v", host, err)
      return nil, err
    }
    break
  }

  if h.proxyURL != nil {
    // We setup the auth header here that is needed to be sent for all requests
    // in the httpconn_client.go
    // TODO(kiran): Should we do this also on 407?
    if u := h.proxyURL.User; u != nil {
      username := u.Username()
      password, _ := u.Password()
      userpass := username + ":" + password
      h.proxyAuthHeader =
        "Basic " + base64.StdEncoding.EncodeToString([]byte(userpass))
      glog.V(2).Infof("proxy auth %s:%s: %s", username, password,
        h.proxyAuthHeader)
    }
    // For TLS, we do the CONNECT request here to setup connection to proxy
    // if a proxy is configured
    if config != nil {
      // https connection, do CONNECT
      connectReq := &http.Request{
        Method: "CONNECT",
        URL:    &url.URL{Opaque: h.url.Host},
        Host:   h.url.Host,
        Header: make(http.Header),
      }
      if h.proxyAuthHeader != "" {
        connectReq.Header.Set("Proxy-Authorization", h.proxyAuthHeader)
      }
      // connect to proxy
      connectReq.Write(conn)
      // read response
      br := bufio.NewReader(conn)
      resp, err := http.ReadResponse(br, connectReq)
      if err != nil {
        glog.V(2).Infof("error in response from proxy %s", h.proxyURL)
        conn.Close()
        return nil, err
      }
      glog.V(2).Infof("response from proxy: %+v", resp)
      if resp.StatusCode != 200 {
        conn.Close()
        return nil, fmt.Errorf("error in proxy response: %v", resp.Status)
      }
    }
  }

  return conn, nil
}

////////////////////////////////////////////////////////////////////////////////
//   Server
////////////////////////////////////////////////////////////////////////////////

// HandleFunc registers a route using the default mux.
func (h *HTTPConn) HandleFunc(pattern string,
  handler func(http.ResponseWriter, *http.Request)) {

  h.defaultHandler.HandleFunc(pattern, handler)
}

// ServeConn provides higher control by processing messages on an already
// accepted connection and delivers the received messages on the channel.
func (h *HTTPConn) ServeConn(conn net.Conn, handler http.Handler,
  reqCh chan *HTTPRcvdReq, rspCh chan *HTTPRcvdRsp, errCh chan error) error {
  if handler == nil {
    handler = h.defaultHandler
  }
  handlerService, err := newHTTPHandlerService(handler)
  if err != nil {
    glog.Errorf("could not create http handler service: %v", err)
    return err
  }
  // Copy the required state from HTTPConn into the server instance.
  server := &httpServer{
    conn:              conn,
    url:               h.url,
    keepAliveInterval: h.keepAliveInterval,
    keepAliveTimeout:  h.keepAliveTimeout,
    rcvdReqCh:         reqCh,
    rcvdRspCh:         rspCh,
    connErrCh:         errCh,
    rcvdReqMap:        util.NewSyncMap(),
    rcvdRPCMap:        util.NewSyncMap(),
    handlerService:    handlerService,
    rpcServer:         h.rpcServerService,
  }

  h.wg.Add(1)
  go func() {
    server.startServer(h.serverQuitCh)
    h.wg.Done()
  }()
  return nil
}

// Listener returns a listener interface that can be passed to Serve
func (h *HTTPConn) Listener() (net.Listener, error) {
  return net.Listen("tcp4", h.url.Host)
}

// Serve starts a Listen-Accept loop to start processing connections and
// deliver the received messages on the supplied message channels.
func (h *HTTPConn) Serve(ln *net.Listener, handler http.Handler) error {

  if h.url.Host == "" {
    return fmt.Errorf("empty Host in Serve for url: %s", h.url)
  }

  if ln == nil {
    listener, errListen := h.Listener()
    if errListen != nil || listener == nil {
      err := fmt.Errorf("error in Listen :: %v, listener %v", errListen,
        listener)
      glog.Errorf(err.Error())
      return err
    }
    ln = &listener
  }

  glog.V(1).Infof("started serving on %v", h.url.Host)

  h.mu.Lock()
  h.listener = *ln
  addr := h.listener.Addr()
  h.wg.Add(1)
  defer h.wg.Done()
  h.mu.Unlock()

  tcpAddr, errAddr := net.ResolveTCPAddr(addr.Network(), addr.String())
  if errAddr != nil {
    glog.Errorf("could not resolve listener address :: %v", errAddr)
    return errAddr
  }
  glog.V(1).Infof("server listening at address: %v", tcpAddr)
  return h.serveLoop(handler, h.rcvdReqCh, h.rcvdRspCh, h.connErrCh)
}

// ListenAndServe function starts an accept loop and starts a httpConn for each
// accepted connection. This is for simple RPC/Handler connections without any
// messages being expected on the channel.
// ln is an optional net.Listener object that can be passed in. If it is nil,
// we will create a new listener.
// TODO(kiran): received messages can cause threads blocking on nil channels.
func (h *HTTPConn) ListenAndServe(
  ln *net.Listener, handler http.Handler) error {

  return h.Serve(ln, handler)
}

// ListenAndServeTLS starts a secure server connection using the CA, cert and
// the key. Compared to standard ListenAndServeTLS, we have a CA param which
// is in addition since we do not rely on the inbuilt CA but look for
// cert and client with self signed certificate.
func (h *HTTPConn) ListenAndServeTLS(caFile, certFile string,
  keyFile string, handler http.Handler) error {

  config, err := libsec.TLSConfigWithCAFromFile(caFile, certFile, keyFile,
    libsec.MasterKey)
  if err != nil || config == nil {
    return err
  }

  h.mu.Lock()
  h.listener, err = tls.Listen("tcp4", h.url.Host, config)
  listener := h.listener
  h.wg.Add(1)
  defer h.wg.Done()
  h.mu.Unlock()
  if err != nil {
    glog.Errorf("error in listen: %v, listener: %v", err, h.listener)
    return err
  }

  glog.Infof("started server")

  func() {
    for {
      conn, err := listener.Accept()
      if err != nil || conn == nil {
        glog.V(2).Infof("error in accept: %v", err)
        return
      }
      glog.Infof("accepted new connection")
      h.ServeConn(conn, handler, nil, nil, nil)
    }
  }()
  return nil
}

// ListenAndServeTLSWithConfig starts a secure server connection using the
// the tls.Config.
func (h *HTTPConn) ListenAndServeTLSWithConfig(handler http.Handler,
  config *tls.Config) error {

  var err error
  h.mu.Lock()
  h.listener, err = tls.Listen("tcp4", h.url.Host, config)
  listener := h.listener
  h.wg.Add(1)
  defer h.wg.Done()
  h.mu.Unlock()
  if err != nil {
    return err
  }

  glog.Infof("started server")

  for {
    conn, err := listener.Accept()
    if err != nil || conn == nil {
      glog.V(2).Infof("error in accept: %v", err)
      return err
    }
    glog.Infof("accepted new connection")
    h.ServeConn(conn, handler, nil, nil, nil)
  }
}

// serveLoop will accept connections and check for temporary and permanent
// errors. For the accepted connections the ServeConn routine is dispatched.
func (h *HTTPConn) serveLoop(handler http.Handler,
  reqCh chan *HTTPRcvdReq, rspCh chan *HTTPRcvdRsp, errCh chan error) error {

  var tempDelay time.Duration // how long to sleep on accept failure
  h.mu.Lock()
  listener := h.listener
  h.wg.Add(1)
  defer h.wg.Done()
  h.mu.Unlock()

  for {
    if listener == nil {
      return fmt.Errorf("nil listener in serveLoop")
    }
    conn, errAcc := listener.Accept()
    if errAcc != nil {
      if ne, ok := errAcc.(net.Error); ok && ne.Temporary() {
        if tempDelay == 0 {
          tempDelay = 5 * time.Millisecond
        } else {
          tempDelay *= 2
        }
        if max := 1 * time.Second; tempDelay > max {
          tempDelay = max
        }
        glog.Infof("accept error: %v; retrying in %v", errAcc, tempDelay)
        time.Sleep(tempDelay)
        continue
      }
      if strings.Contains(errAcc.Error(),
        "use of closed network connection") {
        glog.V(1).Infof("network connection closed on server :: %v", errAcc)
      } else {
        // This will happen when server closes while we are in accept.
        glog.V(1).Infof("error in accept :: ", errAcc)
      }
      return errAcc
    }
    tempDelay = 0
    glog.V(1).Info("accepted new connection")
    h.ServeConn(conn, handler, reqCh, rspCh, errCh)
  }
}

////////////////////////////////////////////////////////////////////////////////

// startServerParser just starts the generic startParser for now.
func startServerParser(conn net.Conn, reqCh chan *http.Request,
  rspCh chan *http.Response, errCh chan error,
  stopCh chan struct{}) error {

  glog.V(2).Info("starting new parser for server")

  return startParser(conn, reqCh, rspCh, errCh, stopCh)
}

// startClientParser just starts the generic startParser for now.
func startClientParser(conn net.Conn, reqCh chan *http.Request,
  rspCh chan *http.Response, errCh chan error,
  stopCh chan struct{}) error {

  glog.V(2).Info("starting new parser for client")

  return startParser(conn, reqCh, rspCh, errCh, stopCh)
}

// startParser blocks on a Read on the connection and converts all received
// messages into requests and responses and puts them on the appropriate
// channels.
func startParser(conn net.Conn, reqCh chan *http.Request,
  rspCh chan *http.Response, errCh chan error,
  stopCh chan struct{}) error {

  if conn == nil {
    err := errors.New("received nil pointer for connection")
    glog.Error(err)
    return err
  }
  var parser httpParser
  var reader *bufio.Reader
  tlsConn, ok := conn.(*tls.Conn)
  if ok {
    glog.V(3).Info("started a tls reader for connection")
    reader = bufio.NewReader(tlsConn)
  } else {
    glog.V(3).Info("started a http reader for connection")
    reader = bufio.NewReader(conn)
  }
  parser = httpParser{Reader: reader}
  // start the parser which will read requests/responses from the
  // connection and send them back to us on the channels.
  return parser.Parse(reqCh, rspCh, errCh, stopCh)
}

// getDuration is used to compute the duration from now to the input deadline.
// If input deadline is zero value then it uses the h.timeout as the duration.
func (h *HTTPConn) getDuration(deadline time.Time) time.Duration {
  var dur time.Duration
  if deadline.IsZero() {
    dur = h.timeout
  } else {
    dur = deadline.Sub(util.GetNowUTC())
    if dur < 0 {
      dur = 0
    }
  }
  return dur
}

// closeClientLocked is called under lock to close client side of connection and
// waits for the stopCh to be signalled from the serveClient loop.
func (h *HTTPConn) closeClientLocked() error {
  if h.httpClient == nil {
    return fmt.Errorf("client already closed")
  }

  close(h.httpClient.quitCh)
  // wait for close to finish processing so we do not nuke state of
  // h.httpClient with next Connect.
  <-h.httpClient.stopCh

  h.httpClient = nil

  return nil
}

// closeClient closes the client side of the connection
func (h *HTTPConn) closeClient() error {
  h.mu.Lock()
  defer h.mu.Unlock()

  return h.closeClientLocked()
}

// closeServer signals the quitch and stops the listener.
func (h *HTTPConn) closeServer() error {
  close(h.serverQuitCh)

  // stop server
  h.mu.Lock()
  defer h.mu.Unlock()
  if h.listener != nil {
    glog.V(3).Infof("closing listener on server %v", h.url)
    errClose := h.listener.Close()
    h.listener = nil
    return errClose
  }
  return nil
}

// Close cleans up the listener and the select loops.
// TODO(kiran): search for elegant stop continues...
func (h *HTTPConn) Close() error {
  if h == nil {
    glog.Error("Close called on invalid connection")
    return nil
  }

  defer h.wg.Wait()

  if h.isClient {
    glog.V(3).Infof("closing connection on client")
    return h.closeClient()
  }

  glog.V(3).Infof("closing connection on server")
  return h.closeServer()
}

// Call is a invocation from a client on a server.
func (h *HTTPConn) Call(serviceMethod string, args interface{},
  reply interface{}) error {
  if h == nil {
    return fmt.Errorf("connection already closed")
  }

  // check the validity of "httpClient" and "rpcClientService"
  // with the lock held.
  h.mu.Lock()
  if h.httpClient == nil || h.rpcClientService == nil {
    h.mu.Unlock()
    return fmt.Errorf("client already closed")
  }
  rpcClientService := h.rpcClientService
  // unlock before making the (potentially long-running) rpc call.
  h.mu.Unlock()
  return rpcClientService.Call(serviceMethod, args, reply)
}
