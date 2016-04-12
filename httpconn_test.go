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
  "errors"
  "flag"
  "fmt"
  "io/ioutil"
  "net"
  "net/http"
  "runtime"
  "strconv"
  "sync"
  "testing"
  "time"

  "github.com/golang/glog"
  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/suite"

  "zerostack/common/libsec"
  "zerostack/common/sysutil"
  "zerostack/common/testutil"
)

type libconnTestSuite struct {
  suite.Suite
}

// TestHTTPConn tests the basic http connection functionality.
func (suite *libconnTestSuite) TestHTTPConn() {

  t := suite.T()
  glog.Info("testing http conn")

  ports, _ := sysutil.FreePortsInRange(50030, 50050, 1)
  assert.Equal(t, len(ports), 1)

  serverAddr := net.JoinHostPort(testGlobal.serverHost,
    strconv.Itoa(int(ports[0])))

  // Start server
  server := newServer()
  server.serve(suite.T(), serverAddr, nil, false, false)
  defer server.closeWait()

  url := "http://" + serverAddr
  timeoutDur := time.Second

  // Create channels  for sending to and receiving from HTTPConn
  send := make(chan *HTTPSendReq, cSendChanSize)
  sendRsp := make(chan *HTTPSendRsp, cSendRspChanSize)
  errCh := make(chan error, cErrChanSize)

  // Create HTTPConn client
  httpConn, err := NewHTTPConnClient(url, timeoutDur, 1, send, sendRsp, errCh,
    0, 0)
  assert.NoError(t, err)
  assert.NotNil(t, httpConn, "HTTPConn cannot be nil for client")

  err = httpConn.Connect()
  assert.NoError(t, err)

  numReq := 1000

  // We are only going to do client side stuff here since we do not expect
  // any server requests.
  for i := 0; i < numReq; i++ {
    send <- &HTTPSendReq{Req: []byte("test" + strconv.Itoa(i))}
  }
  // TODO - check responses are in-order of requests
  for i := 0; i < numReq; i++ {
    select {
    case msg := <-sendRsp:
      glog.V(1).Info("received response on test-client: ", msg.Status)
      assert.NoError(t, msg.Status)

    case err = <-errCh:
      assert.NoError(t, err, "unexpected error in connection")

    case <-time.After(time.Second * 1):
      assert.NotNil(t, nil,
        "unexpected timeout before receiving all responses")
    }
  }
  // test disconnect error handling
  err = httpConn.Close()
  assert.NoError(t, err)

  err = httpConn.Connect()
  assert.NoError(t, err)

  defer httpConn.Close()

  glog.Info("sending second set of messages")

  for i := numReq; i < numReq*2; i++ {
    send <- &HTTPSendReq{Req: []byte("test" + strconv.Itoa(i))}
  }
  for i := 0; i < numReq; i++ {
    select {
    case msg := <-sendRsp:
      glog.V(1).Info("received response: ", msg.Status)
      assert.NoError(t, msg.Status)

    case err := <-errCh:
      assert.NoError(t, err, "unexpected error in connection")

    case <-time.After(time.Second * 2):
      assert.NotNil(t, nil,
        "unexpected timeout before receiving all responses")
    }
  }
}

// TestHTTPConnTimeouts tests the timeout functionality of requests.
func (suite *libconnTestSuite) TestHTTPConnTimeouts() {
  t := suite.T()
  glog.Info("testing http timeouts")

  ports, _ := sysutil.FreePortsInRange(50030, 50050, 1)
  assert.Equal(t, len(ports), 1)

  serverAddr := net.JoinHostPort(testGlobal.serverHost,
    strconv.Itoa(int(ports[0])))

  // Start server
  server := newServer()
  server.serve(suite.T(), serverAddr, nil, true /* drop requests */, false)
  defer server.closeWait()

  url := "http://" + serverAddr
  timeoutDur := time.Second

  // Create channels  for sending to and receiving from HTTPConn
  send := make(chan *HTTPSendReq, cSendChanSize)
  sendRsp := make(chan *HTTPSendRsp, cSendRspChanSize)
  errChan := make(chan error, cErrChanSize)

  // Create HTTPConn client
  httpConn, err := NewHTTPConnClient(url, timeoutDur, 1, send, sendRsp,
    errChan, 0, 0)
  assert.NoError(t, err)
  assert.NotNil(t, httpConn, "HTTPConn cannot be nil for client")

  err = httpConn.Connect()
  assert.NoError(t, err)

  defer httpConn.Close()

  numReq := 1000

  // We are only going to do client side stuff here since we do not expect
  // any server requests.
  for i := 0; i < numReq; i++ {
    send <- &HTTPSendReq{Req: []byte("test" + strconv.Itoa(i))}
  }
  var numRcvd = 0
  for i := 0; i < numReq; i++ {
    select {
    case msg := <-sendRsp:
      glog.V(1).Info("received response: ", msg.Status)
      assert.NotNil(t, msg.Status, "received no error when expecting timeout")
      numRcvd++

    case err := <-errChan:
      assert.NoError(t, err, "unexpected error in connection")

    case <-time.After(time.Second + timeoutDur):
      assert.NotNil(t, nil,
        "did not receive all timeout responses before deadline rcvd: %d",
        numRcvd)
    }
  }
}

// TestHTTPConnKeepAlive tests the keepalive functionality of requests. We
// fake a bad connection by instantiating a client with no keepalives and a
// server with keepalives. The checkKeepAliveServer server should get a
// keepalive error.
func (suite *libconnTestSuite) TestHTTPConnKeepAlive() {
  t := suite.T()
  glog.Info("testing http timeouts")

  ports, _ := sysutil.FreePortsInRange(50030, 50050, 1)
  assert.Equal(t, len(ports), 1)

  serverAddr := testGlobal.serverHost + fmt.Sprintf(":%d", ports[0])

  // Start server
  server := newServer()
  server.serve(suite.T(), serverAddr, nil, false, true /* keepAlive */)
  defer server.closeWait()

  url := "http://" + serverAddr

  // Create channels  for sending to and receiving from HTTPConn
  send := make(chan *HTTPSendReq, cSendChanSize)
  sendRsp := make(chan *HTTPSendRsp, cSendRspChanSize)
  errChan := make(chan error, cErrChanSize)
  timeoutDur := time.Second

  // Create HTTPConn client
  httpConn, err := NewHTTPConnClient(url, timeoutDur, 1, send, sendRsp,
    errChan, 0, 0)
  assert.NoError(t, err)
  assert.NotNil(t, httpConn, "HTTPConn cannot be nil for client")

  err = httpConn.Connect()
  assert.NoError(t, err)

  defer httpConn.Close()

  // send a couple of messages for basic server started sanity check.
  numReq := 2

  // We are only going to do client side stuff here since we do not expect
  // any server requests.
  for i := 0; i < numReq; i++ {
    send <- &HTTPSendReq{Req: []byte("test" + strconv.Itoa(i))}
  }
  var numRcvd = 0
  for i := 0; i < numReq; i++ {
    select {
    case msg := <-sendRsp:
      glog.V(1).Info("received response: ", msg.Status)
      numRcvd++

    case err := <-errChan:
      assert.NoError(t, err, "unexpected error in connection")

    case <-time.After(time.Second + timeoutDur):
      assert.NotNil(t, nil,
        "did not receive all responses before deadline rcvd: %d", numRcvd)
    }
  }

  // checkKeepAliveServer checks for keepalive failure in 3 seconds so give it
  // 5 seconds to fire.
  time.Sleep(5 * time.Second)
}

// Tests the rpcClientService and httpConn by instantiating them individually.
func (suite *libconnTestSuite) TestRPCClientService() {
  t := suite.T()
  glog.Info("testing rpc client")

  ports, _ := sysutil.FreePortsInRange(50030, 50050, 1)
  assert.Equal(t, len(ports), 1)

  serverAddr := net.JoinHostPort(testGlobal.serverHost,
    strconv.Itoa(int(ports[0])))

  // Start server
  server := newServer()
  server.serve(suite.T(), serverAddr, nil, false, false)
  defer server.closeWait()

  url := "http://" + serverAddr
  timeoutDur := 10 * time.Second

  // Create channels  for sending to and receiving from HTTPConn
  send := make(chan *HTTPSendReq, cSendChanSize)
  sendRsp := make(chan *HTTPSendRsp, cSendRspChanSize)
  errChan := make(chan error, cErrChanSize)

  // Create HTTPConn client
  httpConn, errCl := NewHTTPConnClient(url, timeoutDur, 1, send, sendRsp,
    errChan, 0, 0)
  assert.NoError(t, errCl)
  assert.NotNil(t, httpConn, "HTTPConn cannot be nil for client")

  err := httpConn.Connect()
  assert.NoError(t, err)

  defer httpConn.Close()

  // test sync call
  args := "carly"
  var reply []byte
  errRPC := httpConn.Call("RPCPhonebook.FindNumber", args, &reply)
  assert.NoError(t, errRPC, "error in RPC Call")
  assert.Equal(t, reply, []byte{5})
  // non-existent name
  args = "doesnotexist"
  errRPC = httpConn.Call("RPCPhonebook.FindNumber", args, &reply)
  assert.NotNil(t, errRPC, "expected error in RPC Call")
  assert.Equal(t, errRPC, errors.New("unknown name"))
  // nonExportedFunc should fail
  args = "carly"
  errRPC = httpConn.Call("RPCPhonebook.nonExportedFunc", args, &reply)
  assert.NotNil(t, errRPC, "expected error in RPC Call")
  // test async call
  args = "taylor"
  rspChan := make(chan error)
  errRPC = httpConn.Go("RPCPhonebook.FindNumber", args, &reply, rspChan)
  assert.NoError(t, errRPC, "error in RPC Call")

  select {
  case err := <-rspChan:
    assert.NoError(t, err, "error in RPC Call")
    assert.Equal(t, len(reply), 65536)
  case <-time.After(time.Second + timeoutDur):
    assert.NotNil(t, nil, "timeout waiting for RPC response")
  }

  glog.Infof("stopping server")
  server.closeWait()

  glog.Info("waiting for client to notice disconnection")
  httpConn.wg.Wait()

  glog.Infof("doing rpc client call while server is down")

  args = "carly"
  replyCh := make(chan error)
  httpConn.Go("RPCPhonebook.FindNumber", args, &reply, replyCh)

  select {
  case errRPC = <-replyCh:
    assert.Error(t, errRPC)
    glog.Errorln(errRPC.Error())
    assert.Equal(t, ErrClient, errRPC)

  case <-time.After(1 * time.Second):
    assert.Fail(t, "rpc timed out instead of failing")
  }

  // Start server
  server = newServer()
  server.serve(suite.T(), serverAddr, nil, false, false)
  defer server.closeWait()

  glog.Infof("doing rpc client call after reviving server")

  args = "carly"
  errRPC = httpConn.Call("RPCPhonebook.FindNumber", args, &reply)
  assert.Error(t, errRPC)
}

// Tests the simpler interfaces for RPC which just use NewServer
// and NewClient with only RPC interfaces and no messages.
func (suite *libconnTestSuite) TestHTTPRPCClient() {
  t := suite.T()
  glog.Info("testing http rpc client")

  ports, _ := sysutil.FreePortsInRange(50030, 50050, 1)
  assert.Equal(t, len(ports), 1)

  serverAddr := testGlobal.serverHost + fmt.Sprintf(":%d", ports[0])

  // Start server
  server, errInit := NewServer("tcp://" + serverAddr)
  assert.NoError(t, errInit, "could not create rpc server")

  defer server.Close()

  // create client before server is ready. Should fail.
  client, errCl := NewClient("tcp://" + serverAddr)
  assert.NotNil(t, errCl, "expect fail in creating RPC client:: %v", errCl)

  go func() {
    server.ListenAndServe(nil, nil)
  }()

  time.Sleep(cServerStartDelay)

  client, errCl = NewClient("tcp://" + serverAddr)
  assert.NoError(t, errCl, "error in creating RPC client:: %v", errCl)

  defer client.Close()

  // Call() before Register, calls should fail
  args := "carly"
  var reply []byte
  errRPC := client.Call("RPCPhonebook.FindNumber", args, &reply)
  assert.NotNil(t, errRPC, "did not get expected error in RPC Call")

  phonebook := NewRPCPhonebook()
  server.Register(phonebook, "RPCPhonebook")

  numCalls := 100

  var wg sync.WaitGroup
  wg.Add(numCalls)

  for i := 0; i < numCalls; i++ {
    go func(ii int, wg *sync.WaitGroup) {

      defer wg.Done()

      glog.V(2).Infof("started client #%d:", ii)

      clientLoc, errLoc := NewClient("tcp://" + serverAddr)
      assert.NoError(t, errLoc, "error creating RPC client #%d :: %v",
        ii, errLoc)
      assert.NotNil(t, clientLoc)

      defer clientLoc.Close()

      // Call() should succeed
      errCall := clientLoc.Call("RPCPhonebook.FindNumber", args, &reply)
      assert.NoError(t, errCall, "unexpected error in RPC Call: ", errCall)
      assert.Equal(t, reply, []byte{5})
    }(i, &wg)
  }
  wg.Wait()
}

// TestHTTPHandler checks if HTTP calls can be made to a server that is using
// libconn for its server where it can receive either http requests or
// rpc/Bolts on same server code.
// TODO(kiran): Add Bolt requests also in same connection
//              Add http requests on same connection instead of http.Get
func (suite *libconnTestSuite) TestHTTPHandler() {
  t := suite.T()
  glog.Info("testing http handler")

  ports, _ := sysutil.FreePortsInRange(50030, 50050, 1)
  assert.Equal(t, len(ports), 1)

  serverAddr := testGlobal.serverHost + fmt.Sprintf(":%d", ports[0])

  // Start server
  serverURL := "http://" + serverAddr
  rpcServer, err := NewServer("tcp://" + serverAddr)
  assert.NoError(t, err, "could not create rpc server")

  defer rpcServer.Close()

  // setup rpc
  phonebook := NewRPCPhonebook()
  err = rpcServer.Register(phonebook, "RPCPhonebook")
  assert.NoError(t, err, "could not register rpc")

  // setup handler
  expBody := "Handler Tested OK"
  expHdrKey := "TestServer"
  expHdrValue := "Hello World"
  testHandlerFunc := func(rw http.ResponseWriter, req *http.Request) {
    glog.Info("received handler function call")
    rw.Header().Set(expHdrKey, expHdrValue)
    rw.WriteHeader(200)
    rw.Write([]byte(expBody))
  }
  rpcServer.HandleFunc("/tester", testHandlerFunc)

  // start server
  go func() {
    rpcServer.ListenAndServe(nil, nil)
  }()

  time.Sleep(cServerStartDelay)

  // create client
  client, errCl := NewClient("tcp://" + serverAddr)
  assert.NoError(t, errCl, "error creating RPC client")

  defer client.Close()

  // make rpc call
  args := "carly"
  var reply []byte
  errRPC := client.Call("RPCPhonebook.FindNumber", args, &reply)
  assert.NoError(t, errRPC, "unexpected expected error in RPC Call")

  client2, errCl2 := NewClient("tcp://" + serverAddr)
  assert.NoError(t, errCl2, "error creating RPC client")

  defer client2.Close()

  // make rpc call
  args2 := "carly"
  var reply2 []byte
  errRPC2 := client2.Call("RPCPhonebook.FindNumber", args2, &reply2)
  assert.NoError(t, errRPC2, "unexpected expected error in RPC Call")

  // make http request
  request, reqerr := http.NewRequest("GET", serverURL+"/tester", nil)
  assert.NoError(t, reqerr)
  httpClient := &http.Client{}
  response, rsperr := httpClient.Do(request)
  assert.NoError(t, rsperr)
  assert.NotNil(t, response)

  glog.V(2).Info("received response: ", response)

  defer response.Body.Close()
  assert.Equal(t, int64(17), response.ContentLength)
  if response.ContentLength > 0 {
    body, errRd := ioutil.ReadAll(response.Body)
    assert.NoError(t, errRd)
    assert.NotNil(t, body)
    assert.Equal(t, expBody, string(body))
    rcvdHdrValue := response.Header.Get(expHdrKey)
    assert.Equal(t, expHdrValue, rcvdHdrValue)
  }
}

// TestConnectLoop calls connect in a loop to defend against behavior of a
// lib user who might call connect on a connection while previous connection
// is still processing data. This is to stress the internals.
func (suite *libconnTestSuite) TestConnectLoop() {
  t := suite.T()

  glog.Infof("running connect loop test")

  serverConfig, err := libsec.TLSConfigWithCAFromFile(
    testGlobal.caFile,
    testGlobal.serverCertFile,
    testGlobal.serverKeyFile,
    libsec.MasterKey,
  )
  assert.NoError(t, err)
  assert.NotNil(t, serverConfig)

  serverConfig.InsecureSkipVerify = true

  ports, _ := sysutil.FreePortsInRange(50030, 50050, 1)
  assert.Equal(t, len(ports), 1)

  serverAddr := net.JoinHostPort(testGlobal.serverHost,
    strconv.Itoa(int(ports[0])))

  // Start server
  server := newServer()
  server.serve(suite.T(), serverAddr, serverConfig, false, false)
  defer server.closeWait()

  serverURL := "http://" + serverAddr

  // Create channels  for sending to and receiving from HTTPConn
  send := make(chan *HTTPSendReq, cSendChanSize)
  sendRsp := make(chan *HTTPSendRsp, cSendRspChanSize)
  errCh := make(chan error, cErrChanSize)

  clientConfig, err := libsec.TLSConfigWithCAFromFile(
    testGlobal.caFile,
    testGlobal.clientCertFile,
    testGlobal.clientKeyFile,
    libsec.MasterKey,
  )
  assert.NoError(t, err)
  assert.NotNil(t, serverConfig)

  clientConfig.InsecureSkipVerify = true

  timeoutDur := time.Second
  // Create HTTPConn client
  client, err := NewHTTPConnClient(serverURL, timeoutDur, 1, send, sendRsp,
    errCh, 0, 0)
  assert.NoError(t, err)
  assert.NotNil(t, client, "HTTPConn cannot be nil for client")

  var wg sync.WaitGroup
  wg.Add(1)

  go func() {

    defer wg.Done()

    numReq := 1000

    // We are only going to do client side stuff here since we do not expect
    // any server requests.
    for i := 0; i < numReq; i++ {
      send <- &HTTPSendReq{Req: []byte("connectlooptest" + strconv.Itoa(i))}
      time.Sleep(10 * time.Millisecond)
    }
  }()

  // Check that if ConnectTLS gets called in a loop without closing first
  // we can deal with it.
  for i := 0; i < 10; i++ {
    err = client.ConnectTLS(clientConfig)
    assert.NoError(t, err)
    time.Sleep(10 * time.Millisecond)
  }

  wg.Wait()

  // Check that calling Close after Close doesn't barf.
  for i := 0; i < 10; i++ {
    client.Close()
  }
}

func TestLibConnSuite(t *testing.T) {

  flag.Set("stderrthreshold", "0")

  testGlobal.serverHost = "localhost"

  repo := testutil.GetGitRepoRoot()
  assert.NotEqual(t, repo, "")

  configDir := repo + "/conf"
  glog.Infof("setting config dir to %s", configDir)

  // TODO(kiran): Should we use some test certs rather than sky/star certs?
  testGlobal.caFile = configDir + "/zerostack.crt"
  testGlobal.serverCertFile = configDir + "/skygate.crt"
  testGlobal.serverKeyFile = configDir + "/skygate.key"
  testGlobal.clientCertFile = configDir + "/stargate.crt"
  testGlobal.clientKeyFile = configDir + "/stargate.key"

  tester := new(libconnTestSuite)
  suite.Run(t, tester)

  max := 20 + runtime.GOMAXPROCS(0)
  num := runtime.NumGoroutine()
  glog.Infof("number of goroutines: %d", num)

  if num > max {
    buf := make([]byte, 1<<16)
    runtime.Stack(buf, true)
    glog.Infof("%s", buf)
  }
  assert.True(t, num <= max, "found more goroutines %d than max: %d", num, max)
}

// Tests concurrent Close() and Call()
func (suite *libconnTestSuite) TestHTTPRPCClientConcurrentClose() {
  t := suite.T()
  glog.Info("testing http rpc client")

  ports, _ := sysutil.FreePortsInRange(50030, 50050, 1)
  assert.Equal(t, len(ports), 1)

  serverAddr := testGlobal.serverHost + fmt.Sprintf(":%d", ports[0])

  // Start server
  server, errInit := NewServer("tcp://" + serverAddr)
  assert.NoError(t, errInit, "could not create rpc server")

  defer server.Close()

  go func() {
    server.ListenAndServe(nil, nil)
  }()

  time.Sleep(cServerStartDelay)

  phonebook := NewRPCPhonebook()
  server.Register(phonebook, "RPCPhonebook")

  numCalls := 100

  var wg sync.WaitGroup
  wg.Add(2 * numCalls)

  for i := 0; i < numCalls; i++ {
    clientLoc, errLoc := NewClient("tcp://" + serverAddr)
    assert.NoError(t, errLoc, "error creating RPC client #%d :: %v",
      i, errLoc)
    assert.NotNil(t, clientLoc)

    go func(conn *HTTPConn, wg *sync.WaitGroup) {
      defer wg.Done()
      conn.Close()
    }(clientLoc, &wg)

    go func(ii int, conn *HTTPConn, wg *sync.WaitGroup) {
      defer wg.Done()
      glog.V(2).Infof("started client #%d:", ii)

      // Call() should not crash.
      args := "carly"
      var reply []byte
      _ = conn.Call("RPCPhonebook.FindNumber", args, &reply)
    }(i, clientLoc, &wg)
  }
  wg.Wait()
}
