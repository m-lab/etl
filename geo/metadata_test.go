package geo_test

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

	"github.com/m-lab/etl/geo"
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

/* These should move to pt_test
func TestAnnotatePTHops(t *testing.T) {
	tests := []struct {
		hops           []*schema.ParisTracerouteHop
		annotationData map[string]geo.MetaData
		timestamp      time.Time
		res            []*schema.ParisTracerouteHop
	}{
		{
			hops:           nil,
			annotationData: nil,
			timestamp:      epoch,
			res:            nil,
		},
		{
			hops:           []*schema.ParisTracerouteHop{nil},
			annotationData: map[string]geo.MetaData{},
			timestamp:      epoch,
			res:            []*schema.ParisTracerouteHop{nil},
		},
		{
			hops: []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Src_ip: "127.0.0.1"}},
			annotationData: map[string]geo.MetaData{"127.0.0.10": geo.MetaData{
				Geo: &geo.GeolocationIP{}, ASN: nil}},
			timestamp: epoch,
			res: []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Src_ip: "127.0.0.1",
				Src_geolocation: geo.GeolocationIP{}}},
		},
		{
			hops: []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Dest_ip: "1.0.0.127"}},
			annotationData: map[string]geo.MetaData{"1.0.0.1270": geo.MetaData{
				Geo: &geo.GeolocationIP{}, ASN: nil}},
			timestamp: epoch,
			res: []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Dest_ip: "1.0.0.127",
				Dest_geolocation: geo.GeolocationIP{}}},
		},
	}
	for _, test := range tests {
		geo.AnnotatePTHops(test.hops, test.annotationData, test.timestamp)
		if !reflect.DeepEqual(test.hops, test.res) {
			t.Errorf("Expected %s, got %s.", test.res, test.hops)
		}
	}

}

func TestCreateRequestDataFromPTHops(t *testing.T) {
	tests := []struct {
		hops      []*schema.ParisTracerouteHop
		timestamp time.Time
		res       []geo.RequestData
	}{
		{
			hops:      []*schema.ParisTracerouteHop{},
			timestamp: epoch,
			res:       []geo.RequestData{},
		},
		{
			hops:      []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Dest_ip: "1.0.0.127"}},
			timestamp: epoch,
			res:       []geo.RequestData{geo.RequestData{"1.0.0.127", 0, epoch}},
		},
		{
			hops:      []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Src_ip: "127.0.0.1"}},
			timestamp: epoch,
			res:       []geo.RequestData{geo.RequestData{"127.0.0.1", 0, epoch}},
		},
	}
	for _, test := range tests {
		res := geo.CreateRequestDataFromPTHops(test.hops, test.timestamp)
		if !reflect.DeepEqual(res, test.res) {
			t.Errorf("Expected %v, got %v.", test.res, res)
		}
	}
}

func TestAddMetaDataPTHop(t *testing.T) {
	tests := []struct {
		hop       schema.ParisTracerouteHop
		timestamp time.Time
		url       string
		res       schema.ParisTracerouteHop
	}{
		{
			hop:       schema.ParisTracerouteHop{},
			timestamp: time.Now(),
			url:       "/notCalled",
			res:       schema.ParisTracerouteHop{},
		},
		{
			hop:       schema.ParisTracerouteHop{Src_ip: "127.0.0.1"},
			timestamp: time.Now(),
			url:       "/src",
			res: schema.ParisTracerouteHop{
				Src_ip:          "127.0.0.1",
				Src_geolocation: geo.GeolocationIP{Postal_code: "10583"},
			},
		},
		{
			hop:       schema.ParisTracerouteHop{Dest_ip: "127.0.0.1"},
			timestamp: time.Now(),
			url:       "/dest",
			res: schema.ParisTracerouteHop{
				Dest_ip:          "127.0.0.1",
				Dest_geolocation: geo.GeolocationIP{Postal_code: "10583"},
			},
		},
		{
			hop:       schema.ParisTracerouteHop{Src_ip: "127.0.0.1", Dest_ip: "127.0.0.2"},
			timestamp: time.Now(),
			url:       "/both",
			res: schema.ParisTracerouteHop{
				Src_ip:           "127.0.0.1",
				Src_geolocation:  geo.GeolocationIP{Postal_code: "10583"},
				Dest_ip:          "127.0.0.2",
				Dest_geolocation: geo.GeolocationIP{Postal_code: "10583"},
			},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"Geo":{"postal_code":"10583"},"ASN":{}}`)
	}))
	for _, test := range tests {
		geo.BaseURL = ts.URL + test.url
		geo.AddMetaDataPTHop(&test.hop, test.timestamp)
		if !reflect.DeepEqual(test.hop, test.res) {
			t.Errorf("Expected %v, got %v for test %s", test.res, test.hop, test.url)
		}
	}
}
*/

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
		// the geo.
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
		// the geo.
		if err == nil && test.resultError != nil ||
			err != nil && test.resultError == nil {
			t.Errorf("Expected %s, got %s for data: %s\n", test.resultError, err, string(test.testBuffer))
		} else if !reflect.DeepEqual(res, test.resultData) {
			t.Errorf("Expected %+v, got %+v, for data %s\n", test.resultData, res, string(test.testBuffer))
		}
	}
}
