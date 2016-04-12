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
// This file is part of the httpconn with the server-side http handler
// implementation.
//
// Handlers are registered with Handle("route", handlerfunc)
// where handlerfunc has the signature:
// func (http.ResponseWriter, *http.Request) {}
//

package libconn

import (
  "io/ioutil"
  "net/http"
  "net/http/httptest"

  "github.com/golang/glog"
)

// httpHandlerService implements the handler functionality for libconn using the
// handler specified in initialization.
type httpHandlerService struct {
  handler http.Handler
}

// newHTTPHandlerService will return a newly created httpHandlerService
func newHTTPHandlerService(handler http.Handler) (*httpHandlerService, error) {
  return &httpHandlerService{handler: handler}, nil
}

// ServeHTTPReq is the call performed by server (httpconn_server.go) when it
// receives a HTTP request.
func (h *httpHandlerService) ServeHTTPReq(req *http.Request,
  rspChan chan *http.Response) {

  if req == nil || req.URL == nil {
    glog.Errorf("invalid input params")
    return
  }
  if h == nil || h.handler == nil {
    glog.Errorf("cannot handle request, invalid service or handler")
    return
  }
  glog.V(2).Info("received req to route: ", req)
  // Use http recorder functionality to call the handler and get the response.
  rw := httptest.NewRecorder()
  // call the handler
  h.handler.ServeHTTP(rw, req)
  glog.V(2).Infof("handler returned status: %v, header: %s, data: %s",
    rw.Code, rw.HeaderMap, string(rw.Body.Bytes()))
  // When handler returns, rw has the data that needs to be sent as response
  rsp := &http.Response{
    Status:     http.StatusText(rw.Code),
    StatusCode: rw.Code,
    Proto:      req.Proto,
    ProtoMajor: req.ProtoMajor,
    ProtoMinor: req.ProtoMinor,
    Header:     rw.Header(),
    Request:    req,
  }
  length := len(rw.Body.Bytes())
  if length > 0 {
    rsp.Body = ioutil.NopCloser(rw.Body)
    rsp.ContentLength = int64(length)
  } else {
    rsp.ContentLength = 0
  }
  // Send it to httpconn_server.go on the response channel. We are called in a
  // goroutine so blocking here is fine.
  rspChan <- rsp
}
