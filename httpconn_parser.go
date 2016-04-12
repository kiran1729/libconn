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
// Parser for http that parses less HTTP than stdlib but allows for more
// non-web-HTTP like parsing which helps our custom HTTP usage. This parser
// can handle pipelined and duplex channel (same channel sending requests and
// responses).
//
// Example:
//  conn   := net.Dial()
//  reader := bufio.NewReader(conn)
//  parser := httpParser{Reader: reader}
//  go parser.Parse(reqChan, rspChan, errChan)
//
//  for {
//    select {
//    case req := <-reqChan:
//      go handleRequest(req)
//    case rsp := <-rspChan:
//      go handleResponse(rsp)
//    case err := <-errChan:
//      go handleError(err)
//    }
//  }

package libconn

import (
  "bufio"
  "bytes"
  "fmt"
  "github.com/golang/glog"
  "io"
  "io/ioutil"
  "net/http"
  "net/url"
  "strconv"
  "strings"
)

type httpParser struct {
  *bufio.Reader
}

// parseVersion parses and returns an HTTP version string from input buf.
func (h *httpParser) parseVersion(buf string) (major, minor int, err error) {
  if len(buf) < 8 {
    return 0, 0, fmt.Errorf("not enough size for protocol, expect 8")
  }

  proto := buf[0:4]
  if !strings.EqualFold(proto, "HTTP") {
    return 0, 0, fmt.Errorf("invalid protocol %s", proto)
  }

  major = int(int(buf[5]) - '0')
  minor = int(int(buf[7]) - '0')

  if major != 1 || (minor != 0 && minor != 1) {
    return 0, 0, fmt.Errorf("invalid protocol version %d.%d", major, minor)
  }

  return major, minor, nil
}

// parseRequestStart parses the first line of HTTP request.
// e.g."GET /foo HTTP/1.1" into its three parts.
func (h *httpParser) parseRequestStart(line string) (method, requestURI,
  proto string, ok bool) {

  tokens := strings.SplitN(line, " ", 3)
  if len(tokens) != 3 {
    return
  }

  // TODO(kiran): add checks that the fields are really CMD url HTTP/1.1.
  method = tokens[0]
  if method != "GET" && method != "POST" && method != "PUT" &&
    method != "CONNECT" && method != "DELETE" {

    glog.Error("invalid method in request : ", method)
  }

  requestURI = tokens[1]

  proto = tokens[2]
  if len(proto) < 4 {
    return
  }
  if proto[0:4] != "HTTP" {
    return
  }

  return method, requestURI, proto, true
}

// parseResponseStart parses "HTTP/1.1 200 OK" into its three parts.
// Note that some codes(301) do not have associated status message.
func (h *httpParser) parseResponseStart(line string) (major, minor, code int,
  msg string, err error) {

  tokens := strings.SplitN(line, " ", 3)
  if len(tokens) < 2 {
    return 0, 0, 0, "", fmt.Errorf("did not find all response fields")
  }

  major, minor, err = h.parseVersion(tokens[0])
  if err != nil {
    return 0, 0, 0, "", err
  }
  code, err = strconv.Atoi(tokens[1])
  if err != nil {
    return 0, 0, 0, "", err
  }

  if len(tokens) == 3 {
    msg = tokens[2]
  } else {
    msg = ""
  }

  return major, minor, code, msg, nil
}

// readHeader reads one line of HTTP header of key: value format. If the line
// has only \r\n signifying end of headers, it will return true to indicate
// end of header lines.
func (h *httpParser) readHeader() (string, string, bool, error) {
  line, err := h.ReadBytes('\n')
  if err != nil {
    return "", "", false, err
  }
  if line := string(line); line == "\r\n" || line == "\n" {
    return "", "", true, nil
  }
  v := bytes.SplitN(line, []byte(":"), 2)
  if len(v) != 2 {
    return "", "", false, fmt.Errorf("invalid header line: %q", line)
  }
  return string(bytes.TrimSpace(v[0])), string(bytes.TrimSpace(v[1])), false,
    nil
}

// readAllHeaders iterates over all the headers till it hits the end of headers
// section (\r\n\r\n).
func (h *httpParser) readAllHeaders() (*http.Header, error) {
  var err error
  header := http.Header{}

  for {
    var key, value string
    var done bool
    key, value, done, err = h.readHeader()
    if err != nil || done {
      break
    }
    if key == "" {
      // empty header values are valid, rfc 2616 s4.2.
      err = fmt.Errorf("invalid header")
      break
    }
    header.Add(key, value)
  }
  return &header, err
}

// parseContentLength trims whitespace from cl and returns -1 if no value
// is set, or the value if it's >= 0.
func (h *httpParser) parseContentLength(cl string) (int64, error) {
  cl = strings.TrimSpace(cl)
  if cl == "" {
    return -1, nil
  }
  n, err := strconv.ParseInt(cl, 10, 64)
  if err != nil || n < 0 {
    return -1, fmt.Errorf("bad header Content-Length : %s", cl)
  }
  return n, nil
}

// continueParseRequest reads and parses a request after the first line.
func (h *httpParser) continueParseRequest(req *http.Request) error {
  var ok bool
  var err error

  rawURL := req.RequestURI

  req.ProtoMajor, req.ProtoMinor, ok = http.ParseHTTPVersion(req.Proto)
  if !ok {
    return fmt.Errorf("malformed HTTP version: [%s]", req.Proto)
  }

  justAuthority := req.Method == "CONNECT" && !strings.HasPrefix(rawURL, "/")
  if justAuthority {
    rawURL = "http://" + rawURL
  }

  if req.URL, err = url.ParseRequestURI(rawURL); err != nil {
    return err
  }

  if justAuthority {
    // Strip the bogus "http://" back off.
    req.URL.Scheme = ""
  }

  header, err := h.readAllHeaders()
  if err != nil {
    return err
  }
  req.Header = *header

  // RFC2616: Must treat
  //	GET /index.html HTTP/1.1
  //	Host: www.google.com
  // and
  //	GET http://www.google.com/index.html HTTP/1.1
  //	Host: doesntmatter
  // the same.  In the second case, any Host line is ignored.
  req.Host = req.URL.Host
  if req.Host == "" {
    req.Host = req.Header.Get("Host")
  }
  delete(req.Header, "Host")

  length, err := h.parseContentLength(header.Get("Content-Length"))
  if err != nil {
    length = -1
  }

  req.ContentLength = length

  var body []byte
  if length > 0 {
    body = make([]byte, length)
    var readSize int64
    for readSize = 0; readSize < length; {
      count, err := h.Read(body[readSize:])
      if err != nil {
        glog.Error("error reading body: ", err)
        return err
      }
      readSize += int64(count)
      glog.V(2).Info("read bytes: ", count)
    }
    glog.V(2).Info("total read size: ", readSize)
    glog.V(4).Info("read body: ", string(body))
  }

  reader := io.LimitReader(bytes.NewReader(body), req.ContentLength)
  req.Body = ioutil.NopCloser(reader)
  return nil
}

// continueParseResponse parses the response from wire beyond the first line.
func (h *httpParser) continueParseResponse(rsp *http.Response) error {
  header, err := h.readAllHeaders()
  if err != nil {
    return err
  }
  rsp.Header = *header

  length, err := h.parseContentLength(header.Get("Content-Length"))
  if err != nil {
    length = -1
  }

  rsp.ContentLength = length

  var body []byte
  if length > 0 {
    body = make([]byte, length)
    var readSize int64
    for readSize = 0; readSize < length; {
      count, err := h.Read(body[readSize:])
      if err != nil {
        glog.Error("error reading body: ", err)
        return err
      }
      readSize += int64(count)
      glog.V(2).Info("read bytes: ", count)
    }
    glog.V(2).Info("total read size: ", readSize)
    glog.V(4).Info("read body: ", string(body))
  }
  reader := io.LimitReader(bytes.NewReader(body), rsp.ContentLength)
  rsp.Body = ioutil.NopCloser(reader)
  return nil
}

// Parse is the function called by the user on the httpParser(bufio.Reader)
// to read the stream and construct HTTP requests and responses and put them
// on the channels.
// TODO(kiran): any easy way to detect connection closed using bufio?
func (h *httpParser) Parse(reqChan chan *http.Request,
  rspChan chan *http.Response, errChan chan error,
  stopChan chan struct{}) error {

  for {
    select {
    case <-stopChan:
      glog.V(3).Infof("parser stopped")
      return nil
    default:
      break
    }

    line, err := h.ReadBytes('\n')
    if err != nil {
      glog.V(3).Infof("error in parser in read: %v", err)

      select {
      case errChan <- err:
      case <-stopChan:
        glog.V(3).Infof("parser stopped")
      }
      return err
    }
    // Look for a request header first.
    req := new(http.Request)
    var ok bool
    s := string(line)

    glog.V(4).Infof("parsing line: %q", line)

    // drop the \r\n
    s = s[:len(s)-2]
    if (len(s) > 4) && (s[:4] != "HTTP") {
      glog.V(2).Info("parsing request: ", s, "\n")
      req.Method, req.RequestURI, req.Proto, ok = h.parseRequestStart(s)
      if !ok {
        glog.Error("error parsing line: ", s, " err: ", err)
        errChan <- fmt.Errorf("error parsing request: %s", string(line))
        continue
      }
      // continue parsing rest of request
      err = h.continueParseRequest(req)
      if err != nil {
        glog.Error("error parsing request: ", err)
        errChan <- fmt.Errorf("error parsing request: %s", err)
        continue
      }
      reqChan <- req
    } else {
      glog.V(2).Info("parsing response: ", s, "\n")
      // It is not request so let's check for response.
      major, minor, code, msg, err := h.parseResponseStart(s)
      if err != nil {
        glog.Error("error parsing status line: ", s, " err: ", err)
        errChan <- fmt.Errorf("error parsing response: %s, err: %s",
          string(line), err)
        continue
      }
      rsp := new(http.Response)
      rsp.Status = msg
      rsp.StatusCode = code
      rsp.Proto = "HTTP"
      rsp.ProtoMajor = major
      rsp.ProtoMinor = minor
      // TODO(kiran): This barfs on oneline responses without any headers
      // and sequenceid and body.
      err = h.continueParseResponse(rsp)
      if err != nil {
        glog.Error("error parsing response: ", err)
        errChan <- fmt.Errorf("error parsing response: %s", err)
        continue
      }
      rspChan <- rsp
    }
  }
}
