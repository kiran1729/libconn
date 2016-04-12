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
package libconn

import (
  "crypto/tls"
  "errors"
  "net"
  "sync"
  "testing"
  "time"

  "github.com/golang/glog"
  "github.com/stretchr/testify/assert"
)

const (
  cRcvChanSize     int = 10
  cRcvRspChanSize  int = 10
  cSendChanSize    int = 10
  cSendRspChanSize int = 10
  cErrChanSize     int = 10
  // delay to wait for the http go thread to kickin and listen
  cServerStartDelay time.Duration = 2 * time.Second
)

type testVars struct {
  caFile         string
  serverCertFile string
  serverKeyFile  string
  clientCertFile string
  clientKeyFile  string
  serverHost     string
}

var testGlobal testVars

// RPCPhonebook is an interface to be called over RPC. All public methods on
// this interface are registered into rpc.
type RPCPhonebook struct {
  phonebook map[string][]byte
}

// NewRPCPhonebook initializes a dummy RPCPhonebook for testing.
func NewRPCPhonebook() *RPCPhonebook {
  rp := RPCPhonebook{phonebook: make(map[string][]byte)}
  rp.phonebook["carly"] = []byte{5}
  data := make([]byte, 65536)
  rp.phonebook["taylor"] = data
  return &rp
}

// FindNumber takes name as an input and returns the number in the ourput.
func (r *RPCPhonebook) FindNumber(name string, number *[]byte) error {
  if r.phonebook != nil {
    var ok bool
    *number, ok = r.phonebook[name]
    if ok {
      glog.V(1).Infof("found name: %s", name)
      return nil
    }
    glog.V(1).Infof("did not find name: %s", name)
    return errors.New("unknown name")
  }
  glog.Info("empty phonebook")
  return errors.New("unknown name")
}

// nonExportedFunc calls FindNumber just to make sure that a non-exported
// function does not end up working.
func (r *RPCPhonebook) nonExportedFunc(name string, number *[]byte) error {
  return r.FindNumber(name, number)
}

type testServer struct {
  quit     chan struct{}
  wg       sync.WaitGroup
  ln       net.Listener
  httpConn *HTTPConn
}

func newServer() *testServer {
  return &testServer{
    quit: make(chan struct{}),
  }
}

func (s *testServer) closeWait() {
  close(s.quit)
  s.ln.Close()
  s.wg.Wait()
  if s.httpConn != nil {
    s.httpConn.Close()
    s.httpConn = nil
  }
  s.quit = make(chan struct{})
}

// serve starts a server side of the httpConn pipe to receive requests.
// This function starts a server using basic rpcClient and httpConn. Tests that
// use the simpler RPC interface are further down.
// quit - quite the serving loop so new test server can use same port
// drop - whether messages are just dropped after receiving or they
//        are sent responses. This is for testing timeouts.
// keepAlive - starts requests with a keepAliveTimeout. It expects the server
//             to get keepalive error (by starting a client without keepalives
//             to make test easy).
func (s *testServer) serve(t *testing.T, addr string, tlsConfig *tls.Config,
  drop bool, keepAlive bool) {

  var err error
  if tlsConfig == nil {
    s.ln, err = net.Listen("tcp4", addr)
  } else {
    s.ln, err = tls.Listen("tcp4", addr, tlsConfig)
  }
  assert.NoError(t, err)
  assert.NotNil(t, s.ln)

  glog.Infof("started listening at %s", addr)

  if !keepAlive {
    s.httpConn, err = NewServer("http://" + addr)
  } else {
    s.httpConn, err = NewHTTPConnServer("http://"+addr, nil, nil, nil,
      1*time.Second, 3*time.Second)
  }

  assert.NoError(t, err)
  assert.NotNil(t, s.httpConn, "HTTPConn cannot be nil for server")

  phonebook := NewRPCPhonebook()
  errReg := s.httpConn.Register(phonebook, "RPCPhonebook")
  if errReg != nil {
    glog.Error("error registering RPC: ", errReg)
  }

  // In server we are only going to listen to the rcvd channel and send
  // responses back on rcvdRsp channel
  s.wg.Add(1)
  go func() {
    defer s.wg.Done()
    for {
      conn, err := s.ln.Accept()
      if err != nil {
        select {
        case <-s.quit:
          return
        default:
        }
        glog.Error("error in accept: ", err)
        return
      }
      s.wg.Add(1)
      go func() {
        defer s.wg.Done()
        rcvd := make(chan *HTTPRcvdReq, cRcvChanSize)
        rcvdRsp := make(chan *HTTPRcvdRsp, cRcvRspChanSize)
        errChan := make(chan error, cErrChanSize)

        err = s.httpConn.ServeConn(conn, nil, rcvd, rcvdRsp, errChan)
        assert.NoError(t, err)
        for {
          var timeoutCh <-chan time.Time // default nil will be ignored
          if keepAlive {
            timeoutCh = time.After(5 * time.Second)
          }
          select {
          case msg := <-rcvd:
            if msg == nil {
              glog.Error("error in receive channel")
              continue
            }
            glog.V(3).Info("received on test-server: ", string(msg.Req))
            rsp := HTTPRcvdRsp{Req: msg, Status: nil}
            // TODO: randomly reorder responses
            if !drop {
              rcvdRsp <- &rsp
            }

          case err := <-errChan:
            glog.Info("received connection error: ", err)
          case <-timeoutCh:
            assert.Fail(t, "keepalive was not received as expected in less than 5")

          case <-s.quit:
            return
          }
        }
      }()
    }
  }()
}
