// Package test_util provides utilities useful for bigquery
package test_util

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"golang.org/x/oauth2/google"
)

// TODO(gfr) Move these to an http-util package in another repo?
type loggingTransport struct {
	Transport http.RoundTripper
}

type nopCloser struct {
	io.Reader
}

func (nc *nopCloser) Close() error { return nil }

// Log the contents of a reader, returning a new reader with
// same content.
func loggingReader(r io.ReadCloser) io.ReadCloser {
	buf, _ := ioutil.ReadAll(r)
	r.Close()
	log.Printf("Response body:\n%+v\n", string(buf))
	return &nopCloser{bytes.NewReader(buf)}
}

// RoundTrip implements the RoundTripper interface, logging the
// request, and the response body, (which may be json).
func (rt loggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// request includes functions, so we cannot json.Marshal it.
	// Using %#v results in an escaped string we can use in code.
	log.Printf("Request:\n%#v\n", req)
	resp, err := rt.Transport.RoundTrip(req)
	resp.Body = loggingReader(resp.Body)
	return resp, err
}

// LoggingClient is an HTTP client that also logs all requests and
// responses.
// TODO(gfr) Add support for an arbitrary logger.
func LoggingClient() (*http.Client, error) {
	ctx := context.Background()
	client, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/bigquery")
	if err != nil {
		return nil, err
	}

	client.Transport = &loggingTransport{client.Transport}

	return client, nil
}

// channelTransport provides a RoundTripper that handles everything
// locally.
type channelTransport struct {
	//	Transport http.RoundTripper
	Responses <-chan *http.Response
}

// RoundTrip implements the RoundTripper interface, using a channel to
// provide http responses.  This will block if the channel is empty.
func (t channelTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp := <-t.Responses // may block
	resp.Request = req
	return resp, nil
}

// ChannelClient is an HTTP client that ignores requests and returns
// responses provided by a channel.
// responses.
func ChannelClient(c <-chan *http.Response) *http.Client {
	client := &http.Client{}
	client.Transport = &channelTransport{c}

	return client
}
