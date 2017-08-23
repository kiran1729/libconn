# libconn

libconn is a library that provides a HTTP transport multiplexor that is exposed to the user
as a channel of bytestreams and as an RPC provider.

The advantage of libconn connection over using standard http library is the fact that it uses a single
TCP socket over which all the requests and responses flow. The client can also do RPC calls
over the same connection. The most important reason to use this library is to excplicitly control (limit)
the number of socket connections between the client and server.

The library supports
* chan []bytes as the message interface
* RPC calls from client to server
* setting deadlines for requests to error out
* Connection error handling
* HTTP Proxy support
* Pipelined requests
* HTTP level keepalives
