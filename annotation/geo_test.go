package annotation_test

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/m-lab/etl/annotation"
)

var epoch time.Time = time.Unix(0, 0)

func TestFetchGeoAnnotations(t *testing.T) {
	tests := []struct {
		ips       []string
		timestamp time.Time
		geoDest   []*geo.GeolocationIP
		res       []*geo.GeolocationIP
	}{
		{
			ips:       []string{},
			timestamp: epoch,
			geoDest:   []*geo.GeolocationIP{},
			res:       []*geo.GeolocationIP{},
		},
		{
			ips:       []string{"", "127.0.0.1", "2.2.2.2"},
			timestamp: epoch,
			geoDest: []*geo.GeolocationIP{
				&geo.GeolocationIP{},
				&geo.GeolocationIP{},
				&geo.GeolocationIP{},
			},
			res: []*geo.GeolocationIP{
				&geo.GeolocationIP{},
				&geo.GeolocationIP{Postal_code: "10583"},
				&geo.GeolocationIP{},
			},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"127.0.0.10" : {"Geo":{"postal_code":"10583"},"ASN":{}}`+
			`,"2.2.2.20" : {"Geo":null,"ASN":null}}`)
	}))
	for _, test := range tests {
		geo.BatchURL = ts.URL
		geo.FetchGeoAnnotations(test.ips, test.timestamp, test.geoDest)
		if !reflect.DeepEqual(test.geoDest, test.res) {
			t.Errorf("Expected %s, got %s", test.res, test.geoDest)
		}
	}
}

func TestGetAndInsertGeolocationIPStruct(t *testing.T) {
	tests := []struct {
		geo       *geo.GeolocationIP
		ip        string
		timestamp time.Time
		url       string
		res       *geo.GeolocationIP
	}{
		{
			geo:       &geo.GeolocationIP{},
			ip:        "123.123.123.001",
			timestamp: time.Now(),
			url:       "portGarbage",
			res:       &geo.GeolocationIP{},
		},
		{
			geo: &geo.GeolocationIP{},
			ip:  "127.0.0.1",
			url: "/10583",
			res: &geo.GeolocationIP{Postal_code: "10583"},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"Geo":{"postal_code":"10583"},"ASN":{}}`)
	}))
	for _, test := range tests {
		geo.BaseURL = ts.URL + test.url
		geo.GetAndInsertGeolocationIPStruct(test.geo, test.ip, test.timestamp)
		if !reflect.DeepEqual(test.geo, test.res) {
			t.Errorf("Expected %v, got %v for test %s", test.res, test.geo, test.url)
		}
	}

}

func TestGetMetaData(t *testing.T) {
	tests := []struct {
		url string
		res *geo.MetaData
	}{
		{
			url: "portGarbage",
			res: nil,
		},
		{
			url: "/badJson",
			res: nil,
		},
		{
			url: "/goodJson",
			res: &geo.MetaData{},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasSuffix(path, "goodJson") {
			fmt.Fprint(w, `{"Geo":null,"ASN":null}`)
			return
		}
		fmt.Fprint(w, "{jngngfsljngsljngfsljngsljn")
	}))
	for _, test := range tests {
		res := geo.GetMetaData(ts.URL + test.url)
		if res != test.res && *res != *test.res {
			t.Errorf("Expected %+v, got %+v for data: %s\n", test.res, res, test.url)
		}

	}

}

func TestQueryAnnotationService(t *testing.T) {
	tests := []struct {
		url string
		res []byte
		err error
	}{
		{
			url: "portGarbage",
			res: nil,
			err: errors.New("HTTP Protocol Error"),
		},
		{
			url: "/error",
			res: nil,
			err: errors.New("HTTP 404 Error"),
		},
		{
			url: "/Echo",
			res: []byte("Echo"),
			err: nil,
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasSuffix(path, "error") {
			http.Error(w, "Test Error", 404)
			return
		}
		fmt.Fprint(w, "Echo")
	}))
	for _, test := range tests {
		json, err := geo.QueryAnnotationService(ts.URL + test.url)
		if err != nil && test.err == nil || err == nil && test.err != nil {
			t.Errorf("Expected %s, got %s, for %s", test.err, err, test.url)
		}
		if !bytes.Equal(json, test.res) {
			t.Errorf("Expected %s, got %s, for %s", string(test.res), string(json), test.url)
		}

	}
}

func TestParseJSONMetaDataResponse(t *testing.T) {
	tests := []struct {
		testBuffer  []byte
		resultData  *geo.MetaData
		resultError error
	}{
		{
			testBuffer:  []byte(`{"Geo":null,"ASN":null}`),
			resultData:  &geo.MetaData{Geo: nil, ASN: nil},
			resultError: nil,
		},
		{
			testBuffer:  []byte(`"Geo":{},"ASN":{`),
			resultData:  nil,
			resultError: errors.New("Couldn't Parse JSON"),
		},
	}
	for _, test := range tests {
		res, err := geo.ParseJSONMetaDataResponse(test.testBuffer)
		// This big mishmash of if statements is simply
		// checking that if one err is nil, that the other is
		// too. Because error messages can vary, this is less
		// brittle than doing just err == test.resultError. If
		// that is okay, then we just use DeepEqual to compare
		// the structs.
		if err == nil && test.resultError != nil ||
			err != nil && test.resultError == nil {
			t.Errorf("Expected %s, got %s for data: %s\n", test.resultError, err, string(test.testBuffer))
		} else if !reflect.DeepEqual(res, test.resultData) {
			t.Errorf("Expected %+v, got %+v, for data %s\n", test.resultData, res, string(test.testBuffer))
		}
	}
}

func TestGetBatchMetaData(t *testing.T) {
	tests := []struct {
		url string
		res map[string]geo.MetaData
	}{
		{
			url: "portGarbage",
			res: nil,
		},
		{
			url: "/badJson",
			res: nil,
		},
		{
			url: "/goodJson",
			res: map[string]geo.MetaData{"127.0.0.1xyz": {Geo: nil, ASN: nil}},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasSuffix(path, "goodJson") {
			fmt.Fprint(w, `{"127.0.0.1xyz": {"Geo":null,"ASN":null}}`)
			return
		}
		fmt.Fprint(w, "{jngngfsljngsljngfsljngsljn")
	}))
	for _, test := range tests {
		res := geo.GetBatchMetaData(ts.URL+test.url, nil)
		if !reflect.DeepEqual(res, test.res) {
			t.Errorf("Expected %+v, got %+v for data: %s\n", test.res, res, test.url)
		}

	}

}

func TestBatchQueryAnnotationService(t *testing.T) {
	tests := []struct {
		url string
		res []byte
		err error
	}{
		{
			url: "portGarbage",
			res: nil,
			err: errors.New("HTTP Protocol Error"),
		},
		{
			url: "/error",
			res: nil,
			err: errors.New("HTTP 404 Error"),
		},
		{
			url: "/Echo",
			res: []byte("Echo"),
			err: nil,
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.HasSuffix(path, "error") {
			http.Error(w, "Test Error", 404)
			return
		}
		fmt.Fprint(w, "Echo")
	}))
	for _, test := range tests {
		json, err := geo.BatchQueryAnnotationService(ts.URL+test.url, nil)
		if err != nil && test.err == nil || err == nil && test.err != nil {
			t.Errorf("Expected %s, got %s, for %s", test.err, err, test.url)
		}
		if !bytes.Equal(json, test.res) {
			t.Errorf("Expected %s, got %s, for %s", string(test.res), string(json), test.url)
		}

	}
}

func TestBatchParseJSONMetaDataResponse(t *testing.T) {
	tests := []struct {
		testBuffer  []byte
		resultData  map[string]geo.MetaData
		resultError error
	}{
		{
			// Note: This is not testing for corruptedIP
			// addresses. The xyz could be a base36
			// encoded timestamp.
			testBuffer:  []byte(`{"127.0.0.1xyz": {"Geo":null,"ASN":null}}`),
			resultData:  map[string]geo.MetaData{"127.0.0.1xyz": {Geo: nil, ASN: nil}},
			resultError: nil,
		},
		{
			testBuffer:  []byte(`"Geo":{},"ASN":{`),
			resultData:  nil,
			resultError: errors.New("Couldn't Parse JSON"),
		},
	}
	for _, test := range tests {
		res, err := geo.BatchParseJSONMetaDataResponse(test.testBuffer)
		// This big mishmash of if statements is simply
		// checking that if one err is nil, that the other is
		// too. Because error messages can vary, this is less
		// brittle than doing just err == test.resultError. If
		// that is okay, then we just use DeepEqual to compare
		// the structs.
		if err == nil && test.resultError != nil ||
			err != nil && test.resultError == nil {
			t.Errorf("Expected %s, got %s for data: %s\n", test.resultError, err, string(test.testBuffer))
		} else if !reflect.DeepEqual(res, test.resultData) {
			t.Errorf("Expected %+v, got %+v, for data %s\n", test.resultData, res, string(test.testBuffer))
		}
	}
}
