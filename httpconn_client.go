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
// This file is part of the httpconn with the client-side implementation. See
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

// HTTPSendReq is used by library users to send a slice on the
// connection using the send channel.
type HTTPSendReq struct {
  Req      []byte
  Deadline time.Time
}

// HTTPSendRsp is used by the httpconn library to respond to the sender with
// the result of a send.
type HTTPSendRsp struct {
  Req    *HTTPSendReq
  Reply  []byte
  Status error
}

// Internal types which store additional state about the requests.
type sendReqState struct {
  Req   *HTTPSendReq
  ID    int64
  Reply chan interface{}
}

// MinIDFunc is a lambda that can be used to find the earliest ID request in a
// SyncMap.
func MinIDFunc(first interface{}, second interface{}) bool {
  return first.(*sendReqState).ID < second.(*sendReqState).ID
}

// MinRPCIDFunc is a lambda that can be used to find the earliest ID request
// in a SyncMap.
func MinRPCIDFunc(first interface{}, second interface{}) bool {
  return first.(*sendRPCState).ID < second.(*sendRPCState).ID
}

// MinDeadlineFunc is a lambda that can be used to find the minimum deadline
// request in a SyncMap.
func MinDeadlineFunc(first interface{}, second interface{}) bool {
  firstDeadline := first.(*sendReqState).Req.Deadline
  secondDeadline := second.(*sendReqState).Req.Deadline
  return firstDeadline.Before(secondDeadline)
}

// MinRPCDeadlineFunc is a lambda that can be used to find the minimum deadline
// request in a SyncMap.
func MinRPCDeadlineFunc(first interface{}, second interface{}) bool {
  firstDeadline := first.(*sendRPCState).Deadline
  secondDeadline := second.(*sendRPCState).Deadline
  return firstDeadline.Before(secondDeadline)
}

// For RPC, we only have response type since requests are received as Call()
// and stored in sendRPCState.
type httpRPCRsp struct {
  Reply  []byte
  Status error
}

// sendRPCState holds the RPC call info on the client side.
type sendRPCState struct {
  Method   string
  Args     []byte
  ID       int64
  Deadline time.Time
  ReplyCh  chan *httpRPCRsp
}

// makeHTTPPostReq takes a slice of bytes and makes a HTTP POST request with
// appropriate headers and msg as POST data.
func (h *HTTPConn) makeHTTPPostReq(msg []byte, id int64,
  method string) (*http.Request, error) {

  if msg == nil {
    return nil, errors.New("invalid nil msg pointer for makeHTTPPostReq")
  }
  reader := io.LimitReader(bytes.NewReader(msg), int64(len(msg)))

  request, errReq := http.NewRequest("POST", h.url.String(), reader)
  if errReq != nil {
    glog.Error("could not make HTTP request: ", errReq)
    return nil, errReq
  }
  if request == nil {
    err := errors.New("unknown error in makeHTTPRequest")
    glog.Error(err)
    return nil, err
  }
  request.ContentLength = int64(len(msg))
  if id > 0 {
    request.Header.Set(util.ZSSeqIDHeader, strconv.FormatInt(id, 10))
  }
  if method != "" {
    request.Header.Set(gRPCHeader, method)
  }
  if h.proxyAuthHeader != "" {
    request.Header.Set("Proxy-Authorization", h.proxyAuthHeader)
  }
  return request, nil
}

// writeRequest writes the request to the wire. WriteProxy takes care of
// handling TLS/not.
func (h *HTTPConn) writeRequest(req *http.Request) error {
  h.httpClient.connMu.Lock()
  defer h.httpClient.connMu.Unlock()

  if h.httpClient.writer == nil {
    return fmt.Errorf("nil connection to write to")
  }
  return req.WriteProxy(h.httpClient.writer)
}

// recvReqResponse parses the http Response received from the connection and
// creates the HTTPSendRsp that is sent to the channel.
func (h *HTTPConn) recvReqResponse(rsp *http.Response,
  quitCh chan struct{}) error {

  seq := rsp.Header.Get(util.ZSSeqIDHeader)
  glog.V(2).Info("received response seq: ", seq, " status:", rsp.Status,
    " length: ", rsp.ContentLength, " rsp: ", rsp)

  var id int64
  var mReq interface{}
  var err error
  var ok bool

  id, err = strconv.ParseInt(seq, 10, 64)
  if err != nil {
    glog.Errorf("error parsing sequence id: %s :: %v", seq, err)
    if h.sentReqMap.Len() != 1 {
      glog.Errorf("more than one pending sent request so dropping response")
      return err
    }
    // If there is only one in flight, that is our request, just return for
    // first call to the find function.
    findone := func(interface{}, interface{}) bool {
      return true
    }
    var idInf interface{}
    idInf, mReq = h.sentReqMap.FindFunc(findone)
    id = idInf.(int64)
    mReq, ok = h.sentReqMap.Del(id)
  } else {
    mReq, ok = h.sentReqMap.Del(id)
  }

  h.resetSentReqTmr()

  if !ok {
    // This can happen if request was dropped from map because of timeout.
    glog.Warning("received response for non-existing request id: ", id)
    return errors.New("request with id does not exist")
  }
  req, ok := mReq.(*sendReqState)
  if !ok {
    glog.Error("found invalid request type in sentReqMap for id: ", id)
    return errors.New("invalid request type")
  }
  var status error
  // If we managed to complete the call then use error status from server,
  // else use error from rsp.Status
  if rsp.StatusCode == http.StatusOK {
    errHdr := rsp.Header.Get(gErrorHeader)
    if errHdr == "" {
      status = nil
    } else {
      status = errors.New(errHdr)
    }
  } else if rsp.StatusCode == http.StatusProxyAuthRequired {
    glog.Errorf("received proxy authentication error")
    status = errors.New(rsp.Status)
  } else {
    status = errors.New(rsp.Status)
  }

  defer func() {
    if rsp.Body != nil {
      rsp.Body.Close()
    }
  }()

  var body []byte
  var errRd error
  if rsp.ContentLength > 0 {
    // extract Body and create HTTPRcvdReq
    body, errRd = ioutil.ReadAll(rsp.Body)
    if errRd != nil {
      glog.Errorf("error reading body: %v, err: %s", body, errRd)
      return errRd
    }
  }

  glog.V(3).Info("received request response size: ", len(body), " body: ",
    string(body))

  h.wg.Add(1)
  go func() {
    defer h.wg.Done()
    rsp := &HTTPSendRsp{Req: req.Req, Reply: body, Status: status}
    select {
    case <-quitCh:
    case h.sendRspCh <- rsp:
    }
  }()

  h.httpClient.lastRcvdTime = time.Now()

  return nil
}

// TODO(kiran): abstract all the timer expiries into one overloaded function.

// resetPendReqTmr resets pending requests timer to earliest deadline.
func (h *HTTPConn) resetPendReqTmr() error {
  if h.pendReqMap.Len() < 1 {
    if h.pendReqTmr != nil {
      h.pendReqTmr.Stop()
    }
    h.pendReqTmr = nil
    return nil
  }
  pKey, pReq := h.pendReqMap.CmpFunc(MinDeadlineFunc)
  if pReq != nil && (h.pendReqTmr == nil || h.pendReqTmr.Payload != pKey) {
    if h.pendReqTmr != nil {
      h.pendReqTmr.Stop()
    }
    dur := h.getDuration(pReq.(*sendReqState).Req.Deadline)
    h.pendReqTmr = util.NewLoadedTimer(dur, pKey.(int64), h.pendReqTmrCh)
    glog.V(2).Info("setting pending timer to pending request id: ",
      pKey.(int64))
  }
  return nil
}

// addPendingRequest adds the message into a pending request map and starts
// the timer to make sure we do not keep this pending for too long.
func (h *HTTPConn) addPendingRequest(msg *HTTPSendReq) {
  if msg.Deadline.IsZero() {
    msg.Deadline = util.GetNowUTC().Add(h.timeout)
  }
  id := h.pendSeqID.Incr()
  newState := sendReqState{Req: msg, ID: id}
  h.pendReqMap.Add(id, &newState)
  h.resetPendReqTmr()
  glog.V(2).Info("adding to pending requests id: ", id)
}

// resetSentTmr resets pending requests timer to earliest deadline.
func (h *HTTPConn) resetSentReqTmr() error {
  if h.sentReqMap.Len() < 1 {
    if h.sentReqTmr != nil {
      h.sentReqTmr.Stop()
    }
    h.sentReqTmr = nil
    return nil
  }
  pKey, pReq := h.sentReqMap.CmpFunc(MinDeadlineFunc)
  if pReq != nil && (h.sentReqTmr == nil || h.sentReqTmr.Payload != pKey) {
    if h.sentReqTmr != nil {
      h.sentReqTmr.Stop()
    }
    dur := h.getDuration(pReq.(*sendReqState).Req.Deadline)
    h.sentReqTmr = util.NewLoadedTimer(dur, pKey.(int64), h.sentReqTmrCh)
    glog.V(2).Infof("setting sent timer to sent request id: %v dur: %v",
      pKey.(int64), dur)
  }
  return nil
}

// sendRequest creates an HTTP request with the HTTPSendReq bytes and sends it
// on the connection.
func (h *HTTPConn) sendRequest(msg *HTTPSendReq, id int64,
  quitCh chan struct{}) error {

  glog.V(2).Info("sending request")

  // A couple of checks first for cases that do not usually happen but
  // sometimes on startup or client bug.
  if msg == nil {
    h.sentReqMap.Del(id)
    h.resetSentReqTmr()
    return errors.New("nil message in sendRequest")
  }

  if h.httpClient.conn == nil {
    err := ErrInit
    h.wg.Add(1)
    go func() {
      defer h.wg.Done()
      rsp := &HTTPSendRsp{Req: msg, Status: err}
      select {
      case <-quitCh:
      case h.sendRspCh <- rsp:
        if h.connErrCh != nil {
          h.connErrCh <- err
        }
      }
    }()
    h.sentReqMap.Del(id)
    h.resetSentReqTmr()
    return err
  }

  // Stupid error handling is done. Now for handling real request.

  req, errReq := h.makeHTTPPostReq(msg.Req, id, "")
  if errReq != nil {
    h.wg.Add(1)
    go func() {
      defer h.wg.Done()
      rsp := &HTTPSendRsp{Req: msg, Status: errReq}
      select {
      case <-quitCh:
      case h.sendRspCh <- rsp:
      }
    }()
    h.sentReqMap.Del(id)
    h.resetSentReqTmr()
    return errReq
  }

  defer func() {
    if req != nil && req.Body != nil {
      req.Body.Close()
    }
  }()

  glog.V(2).Info("writing request to connection: ", req)

  errWr := h.writeRequest(req)
  if errWr != nil {
    glog.Errorf("error writing to connection for request: %s :: %v",
      string(msg.Req), errWr)
    h.sentReqMap.Del(id)
    h.resetSentReqTmr()
    h.wg.Add(1)
    go func() {
      defer h.wg.Done()
      rsp := &HTTPSendRsp{Req: msg, Status: ErrDisconnect}
      select {
      case <-quitCh:
      case h.sendRspCh <- rsp:
      }
    }()
    return errWr
  }

  h.httpClient.lastSendTime = time.Now()

  return nil
}

// handleSendRequest takes a request from the send channel and does one of:
// - add it to pending queue if HTTP pipeline has too many requests
// - drop it if there are too many pending
// - send it on the HTTP connection and add it to sentReqMap
// - if input msg is nil, then sends one of the pending requests if possible
//
// handleSendRequest executes synchronously from the startClient loop so any
// state updates are inline and heavy lifting is in goroutines.
func (h *HTTPConn) handleSendRequest(msg *HTTPSendReq,
  quitCh chan struct{}) error {

  // We will limit how many requests we will have in flight:
  // For non-pipelined, limit is 1. For pipelined, we will still want to limit
  // to some reasonable number.
  if h.httpClient.conn == nil ||
    (h.sentReqMap.Len()+h.sentRPCMap.Len() >= h.maxPipeReqs) {

    if msg != nil {
      h.addPendingRequest(msg)
    }
    return nil
  }

  // If we are here we know we can send a message if we have one.

  // If we do not have input message, then are there any pending messages?
  if msg == nil && h.pendReqMap.Len() == 0 {
    return nil
  }

  if msg == nil {
    // If we did not get any input message then we look for oldest pending
    // message and send it.
    pKey, pReq := h.pendReqMap.CmpFunc(MinIDFunc)
    if pReq == nil { // nothing in pending
      return nil
    }
    pReq, ok := h.pendReqMap.Del(pKey.(int64))
    h.resetPendReqTmr()
    if !ok {
      glog.Error("unexpected error in deleting from pendReqMap")
      return nil
    }
    req, ok := pReq.(*sendReqState)
    if !ok {
      glog.Error("type mismatch in reading from pendReqMap")
      return nil
    }
    glog.V(2).Info("found a pending request to send with id: ", req.ID)
    msg = req.Req
  }

  // TODO(kiran): Should we have option for messages without deadline?
  // e.g. GET for a command from the cloud? - User can just set Deadline to
  // infinity instead of leaving default zero?
  if msg.Deadline.IsZero() {
    msg.Deadline = util.GetNowUTC().Add(h.timeout)
  }

  id := h.sentSeqID.Incr()
  newState := sendReqState{Req: msg, ID: id}
  h.sentReqMap.Add(id, &newState)
  h.resetSentReqTmr()

  return h.sendRequest(msg, id, quitCh)
}

// handlePendReqTimeout handles timeouts from the requests still pending.
func (h *HTTPConn) handlePendReqTimeout(id int64, quitCh chan struct{}) error {
  pReq, ok := h.pendReqMap.Del(id)
  h.resetPendReqTmr()
  if !ok {
    glog.Warning("timer fired for a non-existent pending message: ", id)
    return nil
  }
  req, ok := pReq.(*sendReqState)
  if !ok {
    err := fmt.Errorf("type mismatch in reading from pendReqMap at id: %d", id)
    glog.Error(err)
    return err
  }
  // send timeout response
  h.wg.Add(1)
  go func() {
    defer h.wg.Done()
    rsp := &HTTPSendRsp{Req: req.Req, Status: ErrTimeout}
    select {
    case <-quitCh:
    case h.sendRspCh <- rsp:
    }
  }()
  return nil
}

// handleSentReqTimeout handles the timers received from messages sent to the
// server.
func (h *HTTPConn) handleSentReqTimeout(id int64, quitCh chan struct{}) error {
  sReq, ok := h.sentReqMap.Del(id)
  h.resetSentReqTmr()
  if !ok {
    // response might have been received before we cleared timer.
    glog.Warning("timer fired for a non-existent sent message")
    return nil
  }
  req, ok := sReq.(*sendReqState)
  if !ok {
    err := fmt.Errorf("type mismatch in reading from sentReqMap at id: %d", id)
    glog.Error(err)
    return err
  }
  // send timeout response
  if h.sendRspCh != nil {
    h.wg.Add(1)
    go func() {
      defer h.wg.Done()
      rsp := &HTTPSendRsp{Req: req.Req, Status: ErrTimeout}
      select {
      case <-quitCh:
      case h.sendRspCh <- rsp:
      }
    }()
  }
  return nil
}

/////////////////////////// RPC related functionality //////////////////////////

// resetPendRPCTmr restarts the pending RPC timer to the one with the earliest
// Deadline.
func (h *HTTPConn) resetPendRPCTmr() error {
  if h.pendRPCMap.Len() < 1 {
    if h.pendRPCTmr != nil {
      h.pendRPCTmr.Stop()
    }
    h.pendRPCTmr = nil
    return nil
  }
  pKey, pRPC := h.pendRPCMap.CmpFunc(MinRPCDeadlineFunc)
  if pRPC != nil && (h.pendRPCTmr == nil || h.pendRPCTmr.Payload != pKey) {
    if h.pendRPCTmr != nil {
      h.pendRPCTmr.Stop()
    }
    dur := h.getDuration(pRPC.(*sendRPCState).Deadline)
    h.pendRPCTmr = util.NewLoadedTimer(dur, pKey.(int64), h.pendRPCTmrCh)
    glog.V(2).Info("setting pending timer to pending rpc id: ", pKey.(int64))
  }
  return nil
}

// resetSentRPCTmr restarts the sent RPCs timer to the one with the earliest
// Deadline.
func (h *HTTPConn) resetSentRPCTmr() error {
  if h.sentRPCMap.Len() < 1 {
    if h.sentRPCTmr != nil {
      h.sentRPCTmr.Stop()
    }
    h.sentRPCTmr = nil
    return nil
  }
  pKey, pRPC := h.sentRPCMap.CmpFunc(MinRPCDeadlineFunc)
  if pRPC != nil && (h.sentRPCTmr == nil || h.sentRPCTmr.Payload != pKey) {
    if h.sentRPCTmr != nil {
      h.sentRPCTmr.Stop()
    }
    dur := h.getDuration(pRPC.(*sendRPCState).Deadline)
    h.sentRPCTmr = util.NewLoadedTimer(dur, pKey.(int64), h.sentRPCTmrCh)
    glog.V(2).Info("setting sent timer to sent rpc id: ",
      pKey.(int64))
  }
  return nil
}

// addPendingRPC adds the rpc call into a pending rpc map and starts
// the timer to make sure we do not keep this pending for too long.
func (h *HTTPConn) addPendingRPC(rpc *sendRPCState) {
  if rpc.Deadline.IsZero() {
    rpc.Deadline = util.GetNowUTC().Add(h.timeout)
  }
  h.pendRPCMap.Add(rpc.ID, rpc)
  h.resetPendRPCTmr()
  glog.V(2).Info("adding to pending rpc id: ", rpc.ID)
}

// sendRPC constructs the RPC HTTP message and writes it to the connection.
func (h *HTTPConn) sendRPC(rpc *sendRPCState, id int64,
  quitCh chan struct{}) error {

  glog.V(3).Info("sending rpc")

  // A couple of checks first for cases that do not usually happen but
  // sometimes on startup or client bug.
  if rpc == nil {
    h.sentRPCMap.Del(id)
    h.resetSentRPCTmr()
    return errors.New("nil rpc in sendRPC")
  }

  if h.httpClient.conn == nil {
    err := ErrInit
    h.wg.Add(1)
    go func() {
      defer h.wg.Done()
      select {
      case <-quitCh:
        return
      case rpc.ReplyCh <- &httpRPCRsp{Status: err}:
      }
      if h.connErrCh != nil {
        h.connErrCh <- err
      }
    }()
    h.sentRPCMap.Del(id)
    h.resetSentRPCTmr()
    return err
  }

  // Stupid error handling is done. Now for handling real request.

  req, errRPC := h.makeHTTPPostReq(rpc.Args, id, rpc.Method)
  if errRPC != nil {
    h.wg.Add(1)
    go func() {
      defer h.wg.Done()
      select {
      case <-quitCh:
      case rpc.ReplyCh <- &httpRPCRsp{Status: errRPC}:
      }
    }()
    h.sentRPCMap.Del(id)
    h.resetSentRPCTmr()
    return errRPC
  }

  defer func() {
    if req != nil && req.Body != nil {
      req.Body.Close()
    }
  }()

  glog.V(2).Info("writing RPC to connection: ", req)

  errWr := h.writeRequest(req)
  if errWr != nil {
    glog.Error("error writing to connection for RPC: ", req)
    h.sentRPCMap.Del(id)
    h.resetSentRPCTmr()
    h.wg.Add(1)
    go func() {
      defer h.wg.Done()
      select {
      case <-quitCh:
      case rpc.ReplyCh <- &httpRPCRsp{Status: ErrDisconnect}:
      }
    }()
    return errWr
  }

  h.httpClient.lastSendTime = time.Now()

  return nil
}

// handleSendRPC receives the rpc from the Call() in httpconn_rpc and sends
// it on the connection or adds it to pending RPCs. If the input rpc is nil,
// handleSendRPC will look in the pending RPCs to see if it can send anything.
func (h *HTTPConn) handleSendRPC(rpc *sendRPCState,
  quitCh chan struct{}) error {

  // If the connection is not initialized (or failed already) then fail rpc
  // fast if it does not have any deadline.
  if h.conn == nil && rpc.Deadline.IsZero() {
    err := ErrInit
    h.wg.Add(1)
    go func() {
      defer h.wg.Done()
      select {
      case <-quitCh:
      case rpc.ReplyCh <- &httpRPCRsp{Status: err}:
      }
      if h.connErrCh != nil {
        h.connErrCh <- err
      }
    }()
    return fmt.Errorf("rpc call while connection is uninitialized")
  }
  // We will limit how many requests we will have in flight:
  // For non-pipelined, limit is 1. For pipelined, we will still want to limit
  // to some reasonable number.
  // TODO(kiran): separate out limits for RPCs and requests
  if h.httpClient.conn == nil ||
    (h.sentReqMap.Len()+h.sentRPCMap.Len() >= h.maxPipeReqs) {

    if rpc != nil {
      h.addPendingRPC(rpc)
    }
    return nil
  }

  if rpc == nil && h.pendRPCMap.Len() == 0 {
    return nil
  }

  if rpc == nil {
    // If we did not get any input message then we look for oldest pending
    // message and send it.
    pKey, pRPC := h.pendRPCMap.CmpFunc(MinRPCIDFunc)
    if pRPC == nil { // nothing in pending
      return nil
    }
    pRPC, ok := h.pendRPCMap.Del(pKey.(int64))
    h.resetPendRPCTmr()
    if !ok {
      glog.Error("unexpected error in deleting from pendRPCMap")
      return nil
    }
    req, ok := pRPC.(*sendRPCState)
    if !ok {
      glog.Error("type mismatch in reading from pendRPCMap")
      return nil
    }
    glog.V(2).Info("found a pending RPC to send with id: ", req.ID)
    rpc = req
  }

  if rpc.Deadline.IsZero() {
    rpc.Deadline = util.GetNowUTC().Add(h.timeout)
  }

  h.sentRPCMap.Add(rpc.ID, rpc)
  h.sendRPC(rpc, rpc.ID, quitCh)

  return nil
}

// recvRPCResponse handles the RPC responses received from the server and
// sends it to the blocked Call().
func (h *HTTPConn) recvRPCResponse(rsp *http.Response,
  quitCh chan struct{}) error {

  seq := rsp.Header.Get(util.ZSSeqIDHeader)
  glog.V(2).Info("received rpc response seq: ", seq, " rsp: ", rsp, " status:",
    rsp.Status)

  id, err := strconv.ParseInt(seq, 10, 64)
  if err != nil {
    glog.Error("error parsing sequence id: ", seq)
    return err
  }

  mReq, ok := h.sentRPCMap.Del(id)
  h.resetSentRPCTmr()
  if !ok {
    // This can happen if request was dropped from map because of timeout.
    glog.Warning("received response for non-existing request id: ", id)
    return ErrTimeout
  }
  rpc, ok := mReq.(*sendRPCState)
  if !ok || rpc == nil {
    glog.Error("found invalid request type in sentRPCMap for id: ", id)
    return errors.New("invalid request type")
  }
  // TODO(kiran): do more detailed error codes?
  var status error
  if rsp.StatusCode == http.StatusOK {
    errHdr := rsp.Header.Get(gErrorHeader)
    if errHdr == "" {
      status = nil
    } else {
      status = errors.New(errHdr)
    }
  } else {
    status = errors.New(rsp.Status)
  }

  defer func() {
    if rsp.Body != nil {
      rsp.Body.Close()
    }
  }()

  var body []byte
  var errRd error
  if rsp.ContentLength > 0 {
    // extract Body and create HTTPRcvdReq
    body, errRd = ioutil.ReadAll(rsp.Body)
    // TODO(kiran): should we still return RPC Call error here?
    if errRd != nil {
      glog.Errorf("error reading body: %v, err: %s", body, errRd)
      return errRd
    }
  }

  h.wg.Add(1)
  go func() {
    defer h.wg.Done()
    select {
    case <-quitCh:
    case rpc.ReplyCh <- &httpRPCRsp{Reply: body, Status: status}:
    }
  }()

  h.httpClient.lastRcvdTime = time.Now()

  return nil
}

// handlePendRPCTimeout handles timeouts from the RPCs still pending.
func (h *HTTPConn) handlePendRPCTimeout(id int64, quitCh chan struct{}) error {
  pRPC, okDel := h.pendRPCMap.Del(id)
  h.resetPendRPCTmr()
  if !okDel {
    glog.Warning("timer fired for a non-existent pending rpc: ", id)
    return nil
  }
  rpc, okType := pRPC.(*sendRPCState)
  if !okType {
    err := fmt.Errorf("type mismatch in reading from pendRPCMap at id: %d", id)
    glog.Error(err)
    return err
  }
  // send timeout response
  h.wg.Add(1)
  go func() {
    defer h.wg.Done()
    select {
    case <-quitCh:
    case rpc.ReplyCh <- &httpRPCRsp{Reply: nil, Status: ErrTimeout}:
    }
  }()
  return nil
}

// handlePendRPC handles timeouts from the RPCs sent without responses.
func (h *HTTPConn) handleSentRPCTimeout(id int64, quitCh chan struct{}) error {
  pRPC, okDel := h.sentRPCMap.Del(id)
  h.resetSentRPCTmr()
  if !okDel {
    glog.Warning("timer fired for a non-existent sent rpc: ", id)
    return nil
  }
  rpc, okType := pRPC.(*sendRPCState)
  if !okType {
    err := fmt.Errorf("type mismatch in reading from sentRPCMap at id: %d", id)
    glog.Error(err)
    return err
  }
  // send timeout response
  h.wg.Add(1)
  go func() {
    defer h.wg.Done()
    select {
    case <-quitCh:
    case rpc.ReplyCh <- &httpRPCRsp{Reply: nil, Status: ErrTimeout}:
    }
  }()
  return nil
}

// handleRecvResponse looks at the headers to figure out if this is an RPC
// response or a message response.
func (h *HTTPConn) handleRecvResponse(rsp *http.Response,
  quitCh chan struct{}) error {

  keepAliveHdr := rsp.Header.Get(util.ZSKeepAliveHeader)
  if keepAliveHdr != "" {
    h.httpClient.lastRcvdTime = time.Now()
    // Nothing else to do for keepalive response
    return nil
  }

  rpcHdr := rsp.Header.Get(gRPCHeader)
  if rpcHdr == "" {
    h.recvReqResponse(rsp, quitCh)
  } else {
    h.recvRPCResponse(rsp, quitCh)
  }
  return nil
}

// checkPending checks for any pending requests and rpcs that should be sent on
// the connection.
// TODO(kiran): should we call this periodically on a ticker to make sure
// things do not timeout even when idle?
func (h *HTTPConn) checkPending(quitCh chan struct{}) {
  h.handleSendRequest(nil, quitCh)
  h.handleSendRPC(nil, quitCh)
}

// sendKeepAlive constructs a blank message with ZSKeepAliveHeader and sends
// on the socket. Message being nil doesn't matter since it is not processed
// and Deadline doesn't matter. KeepAlive messages are violating max
// messages constraint.
func (h *HTTPConn) sendKeepAlive() error {
  id := h.sentSeqID.Incr()
  request, err := http.NewRequest("GET", h.url.String(), nil)
  if err != nil {
    glog.Error("could not make HTTP request: ", err)
    return err
  }
  if request == nil {
    errR := errors.New("unknown error in sendKeepAlive")
    glog.Error(errR)
    return errR
  }
  request.Header.Set(util.ZSSeqIDHeader, strconv.FormatInt(id, 10))
  request.Header.Set(util.ZSKeepAliveHeader, strconv.FormatInt(id, 10))
  if h.proxyAuthHeader != "" {
    request.Header.Set("Proxy-Authorization", h.proxyAuthHeader)
  }

  glog.V(2).Info("writing request to connection: ", request)

  errWr := h.writeRequest(request)
  if errWr != nil {
    glog.Errorf("error writing req to socket: %v :: %v", request, errWr)
  }
  return errWr
}

// checkClientKeepAlive checks the connections liveness state. If we should
// send a keepAlive message in case the connection was idle for some time then a
// blank message is sent. If we have not received any data in cKeepAliveTimeout
// then the connection has an error.
func (h *HTTPConn) checkClientKeepAlive() error {
  glog.V(1).Infof("checking for keepalive with lastSend %v, lastRcvd %v "+
    " interval %v timeout %v", h.httpClient.lastSendTime,
    h.httpClient.lastRcvdTime, h.keepAliveInterval, h.keepAliveTimeout)

  // Check if we should send keep alive if connection is idle.
  if h.keepAliveInterval != 0 &&
    time.Now().Sub(h.httpClient.lastSendTime) > h.keepAliveInterval {

    glog.V(1).Infof("sending keepalive at %v", time.Now())
    h.sendKeepAlive()
  }
  // Check received data time to make sure we are also receiving data.
  if h.keepAliveTimeout != 0 &&
    time.Now().Sub(h.httpClient.lastRcvdTime) > h.keepAliveTimeout {

    glog.Infof("no data rcvd since %v with timeout %v",
      h.httpClient.lastRcvdTime, h.keepAliveTimeout)
    return ErrKeepAlive
  }
  // we would never come into this function with at least one of keepalive
  // non-zero so fine to assume this will be a valid timer.
  h.httpClient.keepAliveTmr.Restart()
  return nil
}

// failSentRequests sends responses back to client for all sent requests when
// the connection is broken.
func (h *HTTPConn) failSentRequests(quitCh chan struct{}) {
  var (
    idInf interface{}
    mReq  interface{}
    id    int64
    ok    bool
  )

  // simple iterator function
  findany := func(interface{}, interface{}) bool {
    return true
  }

  glog.V(1).Infof("failing %d requests", h.sentReqMap.Len())

  for {
    idInf, mReq = h.sentReqMap.FindFunc(findany)
    if idInf == nil || mReq == nil {
      break
    }
    id = idInf.(int64)
    mReq, ok = h.sentReqMap.Del(id)
    if !ok || mReq == nil {
      continue
    }
    req, _ := mReq.(*sendReqState)
    glog.V(2).Infof("failing sent request %d", id)
    h.wg.Add(1)
    go func() {
      defer h.wg.Done()
      rsp := &HTTPSendRsp{Req: req.Req, Reply: nil, Status: ErrDisconnect}
      select {
      case <-quitCh:
      case h.sendRspCh <- rsp:
      }
    }()
  }
}

// failSentRPCs sends responses back to client for all RPCs when
// the connection is broken.
func (h *HTTPConn) failSentRPCs(quitCh chan struct{}) {
  var (
    idInf interface{}
    mReq  interface{}
    id    int64
    ok    bool
  )
  // simple iterator function
  findany := func(interface{}, interface{}) bool {
    return true
  }

  glog.V(1).Infof("failing %d rpcs", h.sentRPCMap.Len())

  for {
    idInf, mReq = h.sentRPCMap.FindFunc(findany)
    if idInf == nil || mReq == nil {
      break
    }
    id = idInf.(int64)
    mReq, ok = h.sentRPCMap.Del(id)
    if !ok || mReq == nil {
      continue
    }
    rpc, _ := mReq.(*sendRPCState)
    glog.V(2).Infof("failing sent rpc %d", id)
    h.wg.Add(1)
    go func() {
      defer h.wg.Done()
      select {
      case <-quitCh:
      case rpc.ReplyCh <- &httpRPCRsp{Reply: nil, Status: ErrDisconnect}:
      }
    }()
  }
}

// cleanupClient stops all client processing and closes connections and
// cleansup all channels.
func (h *HTTPConn) cleanupClient(quitCh chan struct{}) {

  if h.httpClient == nil {
    return
  }

  // If the connection got closed due to server then we will
  // fail the requests on client. We also select quitCh so that
  // it does not block on sending errors if we are asked to quit.
  h.failSentRequests(quitCh)
  h.failSentRPCs(quitCh)

  h.httpClient.connMu.Lock()
  defer h.httpClient.connMu.Unlock()

  if h.httpClient.conn != nil {
    h.httpClient.conn.Close()
    h.httpClient.conn = nil

    if h.httpClient.keepAliveTmr != nil {
      h.httpClient.keepAliveTmr.Close()
      h.httpClient.keepAliveTmr = nil
    }
    // Make parser, RPC calls and any goroutines stop.
    close(h.httpClient.stopCh)
  }
}

// sendClientError sends any errors observed on socket to the observer.
func (h *HTTPConn) sendClientError(err error, quitCh chan struct{}) {
  if h.connErrCh != nil {
    // The caller might ask to quit instead of handling errors so we
    // should pay attention to quit channel here.
    select {
    case <-quitCh:
      glog.V(2).Infof("quitting without sending errors")
    case h.connErrCh <- err:
    }
  }
}

// startClient starts the client loop to write to and read from the connection.
func (h *HTTPConn) startClient(quitCh chan struct{}) error {
  glog.V(1).Info("started client...")

  pReqCh := make(chan *http.Request, cReqChSize)
  pRspCh := make(chan *http.Response, cRspChSize)
  pErrCh := make(chan error, cRspChSize)
  sendReqCh := h.sendReqCh
  sendRPCCh := h.rpcClientService.rpcCh

  conn := h.httpClient.conn
  if conn == nil {
    return fmt.Errorf("received a nil connection for starting client")
  }

  stopCh := h.httpClient.stopCh
  h.wg.Add(1)
  go func() {
    defer h.wg.Done()
    startClientParser(conn, pReqCh, pRspCh, pErrCh,
      stopCh)
  }()

  var kaTmrChan <-chan time.Time
  // start keep alive timer only here instead of in initClient to avoid
  // starting it too early.
  if h.keepAliveInterval != 0 {
    h.httpClient.keepAliveTmr = util.NewRestartTimer(h.keepAliveInterval)
    h.lastSendTime = time.Now()
    h.lastRcvdTime = time.Now()
    kaTmrChan = h.httpClient.keepAliveTmr.C

    glog.V(1).Infof("starting keepalive timer interval %v timeout %v",
      h.keepAliveInterval, h.keepAliveTimeout)
  } else {
    // nil blocks forever
    kaTmrChan = nil
  }

  defer h.cleanupClient(quitCh)

  var err error
  for {
    // limit maximum pending requests we want to read from producers to limit
    // memory usage. Remaining are not dropped but sitting in the input buffer.
    // We will continue reading when already read pending requests timeout.
    if h.sentReqMap.Len() >= h.maxPipeReqs &&
      h.pendReqMap.Len() >= cMaxPendingReqs {
      sendReqCh = nil
    } else {
      sendReqCh = h.sendReqCh
    }

    if h.sentRPCMap.Len() >= h.maxPipeReqs &&
      h.pendRPCMap.Len() >= cMaxPendingReqs {
      sendRPCCh = nil
    } else {
      sendRPCCh = h.rpcClientService.rpcCh
    }

    select {
    case msg := <-sendReqCh:
      glog.V(2).Info("received new request to send on client")
      h.handleSendRequest(msg, quitCh)

    case rsp := <-pRspCh:
      glog.V(2).Info("received a response on client")
      h.handleRecvResponse(rsp, quitCh)
      h.checkPending(quitCh)

    case rpc := <-sendRPCCh:
      rpc.ID = h.rpcSeqID.Incr()
      glog.V(2).Info("received new rpc to send on client: ", rpc.ID)
      h.handleSendRPC(rpc, quitCh)

    case id := <-h.pendReqTmrCh:
      glog.V(2).Info("pending request timeout: ", id)
      intID, _ := id.(int64)
      h.handlePendReqTimeout(intID, quitCh)

    case id := <-h.sentReqTmrCh:
      glog.V(2).Info("sent request timeout: ", id)
      intID, _ := id.(int64)
      h.handleSentReqTimeout(intID, quitCh)

    case id := <-h.pendRPCTmrCh:
      glog.V(2).Info("pending rpc timeout: ", id)
      intID, _ := id.(int64)
      h.handlePendRPCTimeout(intID, quitCh)

    case id := <-h.sentRPCTmrCh:
      glog.V(2).Info("sent rpc timeout: ", id)
      intID, _ := id.(int64)
      h.handleSentRPCTimeout(intID, quitCh)

    case <-kaTmrChan:
      glog.V(3).Info("keepalive timer")
      err = h.checkClientKeepAlive()
      kaTmrChan = h.httpClient.keepAliveTmr.C
      if err != ErrKeepAlive {
        break
      }
      h.sendClientError(err, quitCh)
      return nil

    case err := <-pErrCh:
      if err == io.EOF {
        glog.V(1).Infof("received disconnection error on client: %v", err)
      } else if strings.Contains(err.Error(),
        "use of closed network connection") {
        glog.V(2).Infof("network connection closed on client :: %v", err)
      } else {
        glog.Warningf("received connection error on client: %v", err)
      }
      h.sendClientError(err, quitCh)
      return err

    case <-quitCh:
      glog.V(1).Info("client quitting")
      // we do not send errors when caller has explicitly called quits.
      return nil
    }
  }
}
