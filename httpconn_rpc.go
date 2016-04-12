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
// This file is part of the httpconn package that implements RPC support over
// the connection. See httpconn_main.go for more description.
//
// See httpconn_test.go for example usage from the server and client side.
//
// RPC methods should have a signature which matches:
// func (m *myService) PublicMethod(input, outputPtr) error {}

package libconn

import (
  "encoding/json"
  "errors"
  "fmt"
  "os"
  "reflect"
  "strings"
  "unicode"
  "unicode/utf8"

  "github.com/golang/glog"

  "zerostack/common/util"
)

// rpcMethodType stores information about each method.
type rpcMethodType struct {
  method    reflect.Method
  argType   reflect.Type
  replyType reflect.Type
}

// rpcServiceMap stores information about a registered service. All the methods
// of the service are stored using the map to rpcMethodType.
type rpcServiceMap struct {
  name     string                    // name of rpcService
  rcvr     reflect.Value             // receiver of methods for the rpcService
  rcvrType reflect.Type              // type of the receiver
  method   map[string]*rpcMethodType // registered methods
}

// rpcServerService is the main class that supports the Register method
// using the rpcServiceMap type.
type rpcServerService struct {
  serviceMap *util.SyncMap // map[string]*rpcServiceMap
}

// rpcClientService is the main class that supports the Call/Go methods by
// sending those requests over the rpcCh to an HTTPConn object.
type rpcClientService struct {
  stopCh chan struct{}
  rpcCh  chan *sendRPCState
}

// newRPCServerService creates an rpcServerService object and initializes
// member vars.
func newRPCServerService() (*rpcServerService, error) {
  return &rpcServerService{serviceMap: util.NewSyncMap()}, nil
}

// newRPCClientService creates an rpcClientService object and initializes
// member vars.
func newRPCClientService() (*rpcClientService, error) {
  return &rpcClientService{rpcCh: make(chan *sendRPCState)}, nil
}

// just to make comparison with error type easy.
var typeOfError = reflect.TypeOf((*error)(nil)).Elem()

// Is this an exported (i.e. upper case) name?
func isExported(name string) bool {
  rune, _ := utf8.DecodeRuneInString(name)
  return unicode.IsUpper(rune)
}

// Is this type exported or a builtin?
func isExportedOrBuiltinType(t reflect.Type) bool {
  for t.Kind() == reflect.Ptr {
    t = t.Elem()
  }
  // PkgPath will be non-empty even for an exported type,
  // so we need to check the type name as well.
  return isExported(t.Name()) || t.PkgPath() == ""
}

// Unregister an rpc export object by its name.
func (r *rpcServerService) Unregister(name string) error {
  if _, found := r.serviceMap.Del(name); !found {
    return os.ErrNotExist
  }
  return nil
}

// Register registers a receiver for the RPC service with all its public
// methods. name is a required parameter since we do not want to create
// and use RPC services with the full package names in the registry.
func (r *rpcServerService) Register(receiver interface{}, name string) error {

  if name == "" {
    msg := "name for rpc service cannot be empty"
    glog.Error(msg)
    return errors.New(msg)
  }
  if _, present := r.serviceMap.Get(name); present {
    return errors.New("rpc service already defined: " + name)
  }
  service := new(rpcServiceMap)
  service.name = name
  service.rcvr = reflect.ValueOf(receiver)
  service.rcvrType = reflect.TypeOf(receiver)

  // Install the methods
  service.method = registerMethods(service.rcvrType)

  if len(service.method) == 0 {
    msg := "no exported methods found for service " + name
    glog.Error(msg)
    return errors.New(msg)
  }
  glog.V(1).Infof("registering rpc service: service:%s with numMethods:%v",
    service.name, len(service.method))
  r.serviceMap.Add(service.name, service)
  return nil
}

// registerMethods registers all public methods matching the signature into the
// returned map.
func registerMethods(typ reflect.Type) map[string]*rpcMethodType {
  methods := make(map[string]*rpcMethodType)
  for md := 0; md < typ.NumMethod(); md++ {
    method := typ.Method(md)
    mtype := method.Type
    mname := method.Name
    // Method must be exported.
    if method.PkgPath != "" {
      continue
    }
    // Method needs three inputs: receiver, *args, *reply.
    if mtype.NumIn() != 3 {
      glog.Info("method ", method, " has wrong number of inputs: ", mtype.NumIn())
      continue
    }
    // First arg need not be a pointer.
    argType := mtype.In(1)
    if !isExportedOrBuiltinType(argType) {
      glog.Warning(mname, " argument type not exported: ", argType)
      continue
    }
    // Second arg must be a pointer.
    replyType := mtype.In(2)
    if replyType.Kind() != reflect.Ptr {
      glog.Warning("method ", mname, " reply type not a pointer: ", replyType)
      continue
    }
    // Reply type must be exported.
    if !isExportedOrBuiltinType(replyType) {
      glog.Warning("method ", mname, " reply type not exported: ", replyType)
      continue
    }
    // Method needs one output.
    if mtype.NumOut() != 1 {
      glog.Warning("method ", mname, " has wrong number of outputs: ",
        mtype.NumOut())
      continue
    }
    // The return type of the method must be error.
    if returnType := mtype.Out(0); returnType != typeOfError {
      glog.Warning("method ", mname, " returns ", returnType.String(),
        " not error")
      continue
    }
    methods[mname] = &rpcMethodType{method: method, argType: argType,
      replyType: replyType}
    glog.V(2).Infof("register method: %v %v %v", method, argType, replyType)
  }
  return methods
}

// doCall is the server side handling of the Call from the client. doCall is
// called from httpconn_server.go with the body of the HTTP request supplied as
// the args.
// TODO(kiran): Currently the []byte slices are marshaled and unmarshaled using
// JSON. Need to make it protobuf.
func (r *rpcServerService) doCall(serviceMethod string, args []byte) ([]byte,
  error) {

  glog.V(3).Infof("rpc: doCall to %s", serviceMethod)
  glog.V(4).Infof("rpc: doCall to %s with %v", serviceMethod, args)

  dot := strings.LastIndex(serviceMethod, ".")
  if dot < 0 {
    err := fmt.Errorf("rpc: service/method ill-formed: " + serviceMethod)
    glog.Error(err)
    return nil, err
  }
  serviceName := serviceMethod[:dot]
  methodName := serviceMethod[dot+1:]
  // Look up the request.
  serviceInf, ok := r.serviceMap.Get(serviceName)
  if !ok || serviceInf == nil {
    err := errors.New("rpc: can't find service " + serviceName)
    glog.Error(err)
    return nil, err
  }
  service, okType := serviceInf.(*rpcServiceMap)
  if !okType || service == nil {
    err := errors.New("rpc: unexpected type error for service " + serviceName)
    glog.Error(err)
    return nil, err
  }
  mtype := service.method[methodName]
  if mtype == nil {
    err := errors.New("rpc: can't find method " + serviceMethod)
    glog.Error(err)
    return nil, err
  }
  argv := reflect.New(mtype.argType)
  errJSON := json.Unmarshal(args, argv.Interface())
  if errJSON != nil {
    glog.Error("error in unmarshal: ", errJSON)
    return nil, errJSON
  }
  glog.V(4).Infof("rpc: json unmarshalled request is: %s -> %#v", args, argv)
  replyv := reflect.New(mtype.replyType.Elem())

  glog.V(3).Infof("rpc: calling service %v method %v with %v",
    service, mtype, argv)

  errCall := service.callService(mtype, argv.Elem(), replyv)
  if errCall != nil {
    glog.V(3).Infof("rpc call returned error: ", errCall)
    return nil, errCall
  }
  reply, errRep := json.Marshal(replyv.Interface())
  if errRep != nil {
    glog.Error("rpc reply marshall error: ", errRep)
    return nil, errRep
  }
  glog.V(3).Info("rpc reply: ", string(reply))
  return reply, nil
}

// callService invokes the method of the service.
func (service *rpcServiceMap) callService(mtype *rpcMethodType, argv,
  replyv reflect.Value) error {

  function := mtype.method.Func
  // Invoke the method, providing a new value for the reply.
  returnValues := function.Call([]reflect.Value{service.rcvr, argv, replyv})
  // The return value for the method is an error.
  errInter := returnValues[0].Interface()
  errmsg := ""
  if errInter != nil {
    errmsg = errInter.(error).Error()
    return fmt.Errorf(errmsg)
  }
  return nil
}

// RPC Client functions

// Go is the async call from rpc-client to do the RPC.
// TODO(kiran): Is it a better interface to also put reply into the rspCh
// rather than as a param?
func (r *rpcClientService) Go(serviceMethod string, args interface{},
  reply interface{}, rspCh chan error) error {

  go func() {
    err := r.Call(serviceMethod, args, reply)
    rspCh <- err
  }()
  return nil
}

// Call is the call from the rpc-client to do the RPC.
func (r *rpcClientService) Call(serviceMethod string, args interface{},
  reply interface{}) error {

  if r == nil {
    return fmt.Errorf("error in rpc: client is nil")
  }
  if r.rpcCh == nil {
    return fmt.Errorf("error in rpc client setup: channel is nil")
  }
  buf, errJSON := json.Marshal(args)
  if errJSON != nil {
    glog.Error("error in marshaling args:: ", errJSON)
    return fmt.Errorf("error in marshaling args:: %v", errJSON)
  }

  replyCh := make(chan *httpRPCRsp)
  state := sendRPCState{Method: serviceMethod, Args: buf, ReplyCh: replyCh}

  // send it on the rpc channel to the startClient loop
  glog.V(2).Info("sending rpc on channel: ", serviceMethod)

  select {
  case r.rpcCh <- &state:
    glog.V(2).Info("queued rpc call")
  case <-r.stopCh:
    glog.V(2).Info("abandoning rpc call")
    return ErrClient
  }

  // Now block on the response channel. Timeouts are implemented per request
  // in the client so we do not need to check for timeouts here.
  var rsp *httpRPCRsp
  select {
  case rsp = <-replyCh:
    glog.V(2).Infof("received response for rpc Call")
  case <-r.stopCh:
    glog.V(2).Info("abandoning rpc call after sending")
    return ErrDisconnect
  }

  // This can happen when stopCh gets closed due to connection errors.
  if rsp == nil {
    glog.Error("error in rpc response")
    reply = nil
    return ErrDisconnect
  }
  if rsp.Status != nil {
    return rsp.Status
  }
  glog.V(1).Infof("rpc response succeeded with size: %d", len(rsp.Reply))
  glog.V(3).Infof("rpc response reply: %+v, size: %d", rsp.Reply,
    len(rsp.Reply))
  // success, let's unmarshal
  errRsp := json.Unmarshal(rsp.Reply, reply)
  if errRsp != nil {
    glog.Error("error unmarshaling RPC reply: ", errRsp)
    return errRsp
  }
  return nil
}
