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
  "flag"
  "fmt"
  "net"
  "net/http"
  "net/url"
  "runtime"
  "strconv"
  "sync"
  "sync/atomic"
  "testing"
  "time"

  "github.com/elazarl/goproxy"
  "github.com/elazarl/goproxy/ext/auth"
  "github.com/golang/glog"
  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/suite"

  "zerostack/common/libsec"
  "zerostack/common/sysutil"
  "zerostack/common/testutil"
  "zerostack/common/util"
)

const (
  proxyAddr string = "localhost:50009"
)

type libconnProxyTestSuite struct {
  suite.Suite
  proxy             *goproxy.ProxyHttpServer
  wg                sync.WaitGroup
  listener          net.Listener
  basicAuthorized   int32
  connectAuthorized int32
}

func (suite *libconnProxyTestSuite) SetupSuite() {
  // Start proxy
  glog.Infof("starting proxy server at %s", proxyAddr)
  suite.proxy = goproxy.NewProxyHttpServer()
  if glog.V(1) {
    suite.proxy.Verbose = true
  }
  var lErr error
  suite.listener, lErr = util.Listener4(proxyAddr)
  assert.NoError(suite.T(), lErr)
  suite.wg.Add(1)
  go func() {
    if err := http.Serve(suite.listener, suite.proxy); err != nil {
      glog.Errorf(err.Error())
    }
    suite.wg.Done()
  }()
}

func (suite *libconnProxyTestSuite) TearDownSuite() {
  suite.listener.Close()
  suite.wg.Wait()
}

// TestHTTPProxy tests the proxy http connection functionality.
func (suite *libconnProxyTestSuite) TestHTTPProxy() {

  t := suite.T()
  glog.Info("testing http conn proxy")

  ports, _ := sysutil.FreePortsInRange(50030, 50050, 1)
  assert.Equal(t, len(ports), 1)

  serverAddr := testGlobal.serverHost + fmt.Sprintf(":%d", ports[0])

  // Start server
  server := newServer()
  server.serve(suite.T(), serverAddr, nil, false, false)
  defer server.closeWait()

  serverURL := "http://" + serverAddr
  timeoutDur := time.Second

  // Create channels  for sending to and receiving from HTTPConn
  send := make(chan *HTTPSendReq, cSendChanSize)
  sendRsp := make(chan *HTTPSendRsp, cSendRspChanSize)
  errCh := make(chan error, cErrChanSize)

  // Create HTTPConn client
  client, err := NewHTTPConnClient(serverURL, timeoutDur, 1, send, sendRsp,
    errCh, 0, 0)
  assert.NoError(t, err)
  assert.NotNil(t, client, "HTTPConn cannot be nil for client")

  proxyURL, err := url.Parse("http://" + proxyAddr)
  assert.NoError(t, err)

  err = client.SetProxy(proxyURL)
  assert.NoError(t, err)

  err = client.Connect()
  assert.NoError(t, err)

  // We are only going to do client side stuff here since we do not expect
  // any server requests.
  for i := 0; i < 20; i++ {
    send <- &HTTPSendReq{Req: []byte("test" + strconv.Itoa(i))}
  }
  // TODO - check responses are in-order of requests
  for i := 0; i < 20; i++ {
    select {
    case msg := <-sendRsp:
      glog.Info("received response on test-client: ", msg.Status)
      assert.NoError(t, msg.Status)

    case err = <-errCh:
      assert.NoError(t, err, "unexpected error in connection")

    case <-time.After(time.Second * 1):
      assert.NotNil(t, nil,
        "unexpected timeout before receiving all responses")
    }
  }
  // test disconnect error handling
  err = client.Close()
  assert.NoError(t, err)

  // add a basic auth handler to goproxy
  suite.proxy.OnRequest().Do(auth.Basic("test_realm",
    func(user, passwd string) bool {
      atomic.AddInt32(&suite.basicAuthorized, 1)
      glog.Infof("auth handler called with user [%s] passwd [%s]", user, passwd)
      return user == "alibaba" && passwd == "opensesame"
    }))

  // update proxy url with username:password auth info
  // TODO(kiran): add some tests that fail with incorrect auth?
  // (like in the HTTPS below)
  proxyURL, err = url.Parse("http://alibaba:opensesame@" + proxyAddr)
  assert.NoError(t, err)

  err = client.SetProxy(proxyURL)
  assert.NoError(t, err)

  err = client.Connect()
  assert.NoError(t, err)

  defer client.Close()

  glog.Info("sending second set of messages")

  for i := 20; i < 40; i++ {
    send <- &HTTPSendReq{Req: []byte("test" + strconv.Itoa(i))}
  }
  for i := 0; i < 20; i++ {
    select {
    case msg := <-sendRsp:
      glog.Info("received response: ", msg.Status)
      assert.NoError(t, msg.Status)

    case err := <-errCh:
      assert.NoError(t, err, "unexpected error in connection")

    case <-time.After(time.Second * 2):
      assert.NotNil(t, nil,
        "unexpected timeout before receiving all responses")
    }
  }

  // Check that auth was actually used
  assert.Equal(t, suite.basicAuthorized, int32(20))
}

// TestHTTPSProxy tests the https CONNECT with "basic" auth functionality.
func (suite *libconnProxyTestSuite) TestHTTPSProxy() {

  t := suite.T()
  glog.Info("testing https conn proxy")

  // add a connect handler to proxy
  suite.proxy.OnRequest().HandleConnect(auth.BasicConnect("test_realm",
    func(user, passwd string) bool {
      atomic.AddInt32(&suite.connectAuthorized, 1)
      glog.Infof("auth handler called with user [%s] passwd [%s]", user, passwd)
      return user == "alibaba" && passwd == "opensesame"
    }))

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
  timeoutDur := time.Second

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

  // Create HTTPConn client
  client, err := NewHTTPConnClient(serverURL, timeoutDur, 1, send, sendRsp,
    errCh, 0, 0)
  assert.NoError(t, err)
  assert.NotNil(t, client, "HTTPConn cannot be nil for client")

  // Set incorrect proxy password and check connect fails
  proxyURL, err := url.Parse("http://alibaba:simsim@" + proxyAddr)
  assert.NoError(t, err)

  err = client.SetProxy(proxyURL)
  assert.NoError(t, err)

  // This should fail due to wrong password
  err = client.ConnectTLS(clientConfig)
  assert.Error(t, err)

  // Now test with correct proxy password
  proxyURL, err = url.Parse("http://alibaba:opensesame@" + proxyAddr)
  assert.NoError(t, err)

  err = client.SetProxy(proxyURL)
  assert.NoError(t, err)

  err = client.ConnectTLS(clientConfig)
  assert.NoError(t, err)

  for i := 0; i < 20; i++ {
    send <- &HTTPSendReq{Req: []byte("test" + strconv.Itoa(i))}
  }
  // TODO - check responses are in-order of requests
  for i := 0; i < 20; i++ {
    select {
    case msg := <-sendRsp:
      glog.Info("received response on test-client: ", msg.Status)
      assert.NoError(t, msg.Status)

    case err = <-errCh:
      assert.NoError(t, err, "unexpected error in connection")

    case <-time.After(time.Second * 1):
      assert.NotNil(t, nil,
        "unexpected timeout before receiving all responses")
    }
  }

  glog.Infof("closing connection")

  // test disconnect error handling
  err = client.Close()
  assert.NoError(t, err)

  glog.Infof("reconnecting after close")

  err = client.ConnectTLS(clientConfig)
  assert.NoError(t, err)

  defer client.Close()

  glog.Info("sending second set of messages")

  for i := 20; i < 40; i++ {
    send <- &HTTPSendReq{Req: []byte("test" + strconv.Itoa(i))}
  }
  for i := 0; i < 20; i++ {
    select {
    case msg := <-sendRsp:
      glog.Info("received response: ", msg.Status)
      assert.NoError(t, msg.Status)

    case err := <-errCh:
      assert.NoError(t, err, "unexpected error in connection")

    case <-time.After(time.Second * 2):
      assert.NotNil(t, nil,
        "unexpected timeout before receiving all responses")
    }
  }

  // Check that CONNECT auth was actually used
  assert.Equal(t, suite.connectAuthorized, int32(3))
}

// TestLibConnProxyTestSuite tests the proxy functionality
func TestLibConnProxySuite(t *testing.T) {
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

  tester := new(libconnProxyTestSuite)
  suite.Run(t, tester)

  // couple of extra from ListenAndServe of goproxy than httpconn_test.go
  max := 17 + runtime.GOMAXPROCS(0)
  num := runtime.NumGoroutine()

  glog.Infof("number of goroutines: %d", num)

  if num > max {
    buf := make([]byte, 1<<16)
    runtime.Stack(buf, true)
    glog.Infof("%s", buf)
  }
  assert.True(t, num <= max, "found more goroutines %d than max: %d", num, max)
}
