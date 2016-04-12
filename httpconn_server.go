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
// This file is part of the httpconn with the server-side implementation. See
// description in httpconn_main.go.

package libconn

import (
  "bytes"
  "errors"
  "fmt"
  "io"
  "io/ioutil"
  "net/http"
  "strconv"
  "strings"
  "time"

  "github.com/golang/glog"
  "zerostack/common/util"
)

// HTTPRcvdReq is the request received from connection and send to lib user.
// id is immutable from outside so only the getter is public.
type HTTPRcvdReq struct {
  Req   []byte            // the bytes sent by the client
  id    int64             // id which is used to match responses
  RspCh chan *HTTPRcvdRsp // the channel on which response is sent for this req
}

// ID returns the sequence id of the received message.
func (h *HTTPRcvdReq) ID() int64 {
  return h.id
}

// setID sets the request id. It is only used internally.
func (h *HTTPRcvdReq) setID(id int64) {
  h.id = id
}

// HTTPRcvdRsp is used by callee to respond to a received message from the
// connection. httpconn will send this back on the connection as a response.
type HTTPRcvdRsp struct {
  Req    *HTTPRcvdReq // pointer to the original request this response is for
  Reply  []byte       // response body that should be sent (can be nil)
  Status error        // error status which is sent to client (can be nil)
}

// rcvdRPCState holds the RPC call info on the server side.
type rcvdRPCState struct {
  Method string        // method targeting by the rpc
  Req    *http.Request // the original rpc request
  ID     int64         // id to match up responses
  Reply  []byte        // reply params of the rpc call
  Status error         // rpc call result status
}

// makeHTTPRsp constructs a http response from the error code.
func (h *httpServer) makeHTTPRsp(req *http.Request, reply []byte,
  status error) (*http.Response, error) {

  var statusStr string
  var statusCode int
  statusStr = fmt.Sprintf("%d %s", http.StatusOK,
    http.StatusText(http.StatusOK))
  statusCode = http.StatusOK
  rsp := &http.Response{
    Status:     statusStr,
    StatusCode: statusCode,
    Proto:      req.Proto, // HTTP Protocol
    ProtoMajor: req.ProtoMajor,
    ProtoMinor: req.ProtoMinor,
    Header:     make(http.Header),
    Request:    req,
  }

  if len(reply) > 0 {
    rsp.Body = ioutil.NopCloser(bytes.NewBuffer(reply))
    rsp.ContentLength = int64(len(reply))
  } else {
    rsp.ContentLength = 0
  }

  var idHdr, rpcHdr, errHdr string
  if idHdr = req.Header.Get(util.ZSSeqIDHeader); idHdr != "" {
    rsp.Header.Add(util.ZSSeqIDHeader, idHdr)
  }

  if rpcHdr = req.Header.Get(gRPCHeader); rpcHdr != "" {
    rsp.Header.Add(gRPCHeader, rpcHdr)
  }
  if status == nil {
    errHdr = ""
  } else {
    errHdr = status.Error()
  }
  if idHdr != "" || rpcHdr != "" {
    rsp.Header.Add(gErrorHeader, errHdr)
  }
  return rsp, nil
}

// recvRequest reads a request from the connection and sends it to the channel
// for the server.
func (h *httpServer) recvRequest(req *http.Request) error {
  seq := req.Header.Get(util.ZSSeqIDHeader)
  id, errParse := strconv.ParseInt(seq, 10, 64)
  if errParse != nil {
    glog.Errorf("error parsing sequence id: %s :: %v", seq, errParse)
    return errParse
  }
  glog.V(3).Info("received seq: ", seq, " length: ", req.ContentLength,
    " method: ", req.Method, " req: ", req)

  defer req.Body.Close()
  // extract Body and create HTTPRcvdReq
  body, errRd := ioutil.ReadAll(req.Body)
  if errRd != nil {
    glog.Errorf("error reading body: %v :: %s", body, errRd)
    return errRd
  }

  glog.V(3).Info("received size: ", len(body))
  glog.V(4).Info("received body: ", string(body))

  h.rcvdReqMap.Add(id, req)
  msg := &HTTPRcvdReq{Req: body, id: id, RspCh: h.rcvdRspCh}
  select {
  case h.rcvdReqCh <- msg:
  case <-h.stopCh:
  }
  return nil
}

// writeResponse takes an exclusive lock on the connection and writes the
// bytes to the socket.
func (h *httpServer) writeResponse(rsp *http.Response) error {

  h.connMu.Lock()
  defer h.connMu.Unlock()

  if h.conn == nil {
    return fmt.Errorf("nil conn in writing response")
  }

  h.lastDataTime = time.Now()

  wr := io.Writer(h.conn)
  return rsp.Write(wr)
}

// sendResponse creates a http Response and sends it on the connection.
func (h *httpServer) sendResponse(msg *HTTPRcvdRsp, errCh chan error) error {

  if msg == nil || msg.Req == nil || h.conn == nil {
    return errors.New("nil response message pointer")
  }
  id := msg.Req.ID()
  rcvdInf, ok := h.rcvdReqMap.Del(id)
  if !ok || rcvdInf == nil {
    glog.Error("did not find request for response: ", id)
    return errors.New("did not find request for response")
  }
  rcvd, ok := rcvdInf.(*http.Request)
  if !ok || rcvd == nil {
    glog.Error("unexpected type error in rcvdReqMap")
    return errors.New("unexpected type error")
  }
  rsp, errRsp := h.makeHTTPRsp(rcvd, msg.Reply, msg.Status)
  if errRsp != nil || rsp == nil {
    return errRsp
  }
  errWr := h.writeResponse(rsp)
  if errWr != nil {
    glog.Errorf("error writing to connection for request :: %v", errWr)
    h.rcvdReqMap.Del(id)
    go func() {
      select {
      case errCh <- errWr:
      case <-h.stopCh:
      }
    }()
    return errWr
  }
  return nil
}

// recvRPC receives the RPC message from the connection and performs the doCall
// synchronously and sends the response back to startServer loop on the
// rpcRSPCh.
func (h *httpServer) recvRPC(req *http.Request, rpcRspCh chan int64) error {
  serviceMethod := req.Header.Get(gRPCHeader)
  seq := req.Header.Get(util.ZSSeqIDHeader)
  glog.V(2).Info("received seq: ", seq, " req: ", req)

  id, errParse := strconv.ParseInt(seq, 10, 64)
  if errParse != nil {
    glog.Errorf("error parsing sequence id: %s :: %v", seq, errParse)
    return errParse
  }

  defer req.Body.Close()
  body, errRd := ioutil.ReadAll(req.Body)
  if errRd != nil {
    glog.Errorf("error reading body: %v :: %s", body, errRd)
    // TODO(kiran): send error response.
    return errRd
  }

  reply, errCall := h.rpcServer.doCall(serviceMethod, body)
  glog.V(1).Infof("RPC call result err: %v", errCall)
  glog.V(3).Infof("RPC call result data: %v", string(reply))

  state := rcvdRPCState{Method: serviceMethod, Req: req, ID: id, Reply: reply,
    Status: errCall}
  h.rcvdRPCMap.Add(id, &state)

  select {
  case rpcRspCh <- id:
  case <-h.stopCh:
  }
  return nil
}

// sendRPCResponse creates a http Response for an RPC and sends it on the
// connection.
func (h *httpServer) sendRPCResponse(rpcID int64, errCh chan error) error {

  rpcInf, ok := h.rcvdRPCMap.Del(rpcID)
  if !ok || rpcInf == nil {
    glog.Error("did not find rcvdRPCMap id: ", rpcID)
    return errors.New("unexpected rpc error")
  }
  rpc, okType := rpcInf.(*rcvdRPCState)
  if !okType || rpc == nil {
    glog.Error("unexpected type in rcvdRPCMap")
    return errors.New("unexpected type error")
  }
  rsp, errRsp := h.makeHTTPRsp(rpc.Req, rpc.Reply, rpc.Status)
  if errRsp != nil || rsp == nil {
    glog.Error("error making http response: ", errRsp)
    return errRsp
  }

  errWr := h.writeResponse(rsp)

  if errWr != nil {
    glog.Errorf("error writing response for rpc id: %v :: %v",
      rpcID, errWr)
    go func() {
      select {
      case errCh <- errWr:
      case <-h.stopCh:
      }
    }()
    return errWr
  }
  return nil
}

// recvKeepAlive sends a keepAlive message response back to client.
func (h *httpServer) recvKeepAlive(req *http.Request, errCh chan error) error {
  seq := req.Header.Get(util.ZSSeqIDHeader)
  id, errParse := strconv.ParseInt(seq, 10, 64)
  if errParse != nil {
    glog.Errorf("error parsing sequence id: %s :: %v", seq, errParse)
    return errParse
  }
  glog.V(3).Info("received seq: ", seq, " length: ", req.ContentLength,
    " method: ", req.Method, " req: ", req)

  rsp, errRsp := h.makeHTTPRsp(req, nil, nil)
  if errRsp != nil || rsp == nil {
    glog.Error("error making http response: ", errRsp)
    return errRsp
  }

  rsp.Header.Set(util.ZSKeepAliveHeader, strconv.FormatInt(id, 10))

  errWr := h.writeResponse(rsp)
  if errWr != nil {
    glog.Errorf("error writing response for keepalive id: %v :: %v", id, errWr)
    go func() {
      select {
      case errCh <- errWr:
      case <-h.stopCh:
      }
    }()
    return errWr
  }
  return nil
}

// handleRecvRequest reads a HTTP request from the connection and decides
// whether it should be handled as RPC or a message based on the headers.
func (h *httpServer) handleRecvRequest(req *http.Request, rpcRspCh chan int64,
  handlerRspCh chan *http.Response, errCh chan error) error {

  h.lastDataTime = time.Now()

  keepAliveHdr := req.Header.Get(util.ZSKeepAliveHeader)
  if keepAliveHdr != "" {
    go h.recvKeepAlive(req, errCh)
    return nil
  }

  rpcHdr := req.Header.Get(gRPCHeader)
  if rpcHdr != "" {
    go h.recvRPC(req, rpcRspCh)
    return nil
  }

  seqHdr := req.Header.Get(util.ZSSeqIDHeader)
  // If there is a valid channel and a valid sequence id header then we can
  // send it to the channel.
  if h.rcvdReqCh != nil && seqHdr != "" {
    go h.recvRequest(req)
    return nil
  }

  // This is not a request sent by libconn, so look for HTTP HandlerFunc
  go h.handlerService.ServeHTTPReq(req, handlerRspCh)
  return nil
}

// checkServerKeepAlive checks the connections liveness state. If we have not
// received any data in cKeepAliveTimeout then the connection has an error.
func (h *httpServer) checkServerKeepAlive() error {
  if h.keepAliveTimeout != 0 &&
    time.Now().Sub(h.lastDataTime) > h.keepAliveTimeout {

    glog.Infof("keepalive no data rcvd since %v with timeout %v",
      h.lastDataTime, h.keepAliveTimeout)
    return ErrKeepAlive
  }
  return nil
}

// cleanupServer closes the connection under lock and sends notification to
// goroutines and caller by closing stopCh.
func (h *httpServer) cleanupServer() {
  // httpServer has its own WaitGroup, unlike httpConn
  defer h.wg.Wait()

  h.connMu.Lock()
  defer h.connMu.Unlock()

  if h.conn != nil {
    glog.V(1).Infof("cleanup connection on server")
    h.conn.Close()
    h.conn = nil
  }

  close(h.stopCh)

  if h.keepAliveTmr != nil {
    h.keepAliveTmr.Close()
    h.keepAliveTmr = nil
  }
}

// sendServerError informs any receiver about errors on the channel. It pays
// attention to quitCh to make sure caller quit is handled while trying to send
// errors.
func (h *httpServer) sendServerError(err error, quitCh chan struct{}) {
  if err == io.EOF {
    glog.V(2).Infof("received connection error on server: %v", err)
  } else if strings.Contains(err.Error(),
    "use of closed network connection") {
    glog.Infof("network connection closed on server :: %v", err)
  } else if strings.Contains(err.Error(),
    "connection reset by peer") {
    glog.V(2).Infof("client disconnected on server :: %v", err)
  } else if err == ErrKeepAlive {
    glog.Errorf("received keepalive connection error on server: %v", err)
  } else {
    glog.Warningf("received connection error on server: %v", err)
  }
  // TODO(kiran): Ideally, we also want to drain all error responses in
  // the channel. Since the error is received from Parse rather than the
  // goroutine, we do not expect a lot of errors in the errCh which
  // cause reconnect storm.
  if h.connErrCh != nil {
    select {
    case h.connErrCh <- err:
    case <-quitCh:
    }
  }
}

// startServer starts the server loop to write to and read from the connection.
func (h *httpServer) startServer(quitCh chan struct{}) error {

  glog.V(1).Info("started server...")

  pReqCh := make(chan *http.Request, cReqChSize)
  pRspCh := make(chan *http.Response, cRspChSize)
  pErrCh := make(chan error, cRspChSize)

  rspCh := h.rcvdRspCh
  rpcRspCh := make(chan int64)
  handlerRspCh := make(chan *http.Response)

  glog.V(1).Info("new server connection received")
  h.stopCh = make(chan struct{})
  conn := h.conn
  h.wg.Add(1)
  go func() {
    startServerParser(conn, pReqCh, pRspCh, pErrCh, h.stopCh)
    h.wg.Done()
  }()
  defer h.cleanupServer()
  rspCh = h.rcvdRspCh

  // nil by default, blocks forever
  var kaTmrChan <-chan time.Time
  // start keep alive timer only here instead of in initClient to avoid
  // starting it too early.
  if h.keepAliveInterval != 0 {
    h.keepAliveTmr = util.NewRestartTimer(h.keepAliveInterval)
    h.lastDataTime = time.Now()
    kaTmrChan = h.keepAliveTmr.C

    glog.Infof("starting keepalive timer interval %v timeout %v",
      h.keepAliveInterval, h.keepAliveTimeout)
  }

  for {
    // TODO(kiran): do we need to keep rspCh nil if we get any connection
    // errors till connection is fixed?
    select {
    case rsp := <-rspCh:
      glog.V(2).Info("received response for request: ", rsp.Status)
      h.sendResponse(rsp, pErrCh)

    case rpcID := <-rpcRspCh:
      glog.V(2).Info("received response for rpc id: ", rpcID)
      h.sendRPCResponse(rpcID, pErrCh)

    case req := <-pReqCh:
      glog.V(2).Info("received request on server")
      h.handleRecvRequest(req, rpcRspCh, handlerRspCh, pErrCh)

    case rsp := <-handlerRspCh:
      glog.V(2).Info("received handler response on server")
      // TODO(kiran): what do we do on error?
      h.writeResponse(rsp)

    case <-kaTmrChan:
      glog.V(3).Info("keepalive timer")
      err := h.checkServerKeepAlive()
      if err != ErrKeepAlive {
        // note kaTmrChan is actually h.keepAliveTmr.C
        // so restarting keepAliveTmr is rearming this case
        h.keepAliveTmr.RestartExpired()
        break
      }
      h.sendServerError(err, quitCh)
      return nil

    case err := <-pErrCh:
      glog.V(2).Infof("received connection error on server %s: %v", h.url, err)
      rspCh = nil
      h.sendServerError(err, quitCh)
      return err

    case <-quitCh:
      glog.V(1).Info("server quitting")
      return nil
    }
  }
}
