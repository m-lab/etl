package annotation_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-test/deep"

	"github.com/m-lab/annotation-service/api"
	v2 "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/annotation"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

var epoch time.Time = time.Unix(0, 0)

func TestAddGeoAnnotations(t *testing.T) {
	tests := []struct {
		ips       []string
		timestamp time.Time
		geoDest   []*api.GeolocationIP
		res       []*api.GeolocationIP
	}{
		{
			ips:       []string{},
			timestamp: epoch,
			geoDest:   []*api.GeolocationIP{},
			res:       []*api.GeolocationIP{},
		},
		{
			ips:       []string{"", "127.0.0.1", "2.2.2.2"},
			timestamp: epoch,
			geoDest: []*api.GeolocationIP{
				&api.GeolocationIP{},
				&api.GeolocationIP{},
				&api.GeolocationIP{},
			},
			res: []*api.GeolocationIP{
				&api.GeolocationIP{},
				&api.GeolocationIP{PostalCode: "10583"},
				&api.GeolocationIP{},
			},
		},
	}
	responseJSON := `{"AnnotatorDate":"2018-12-05T00:00:00Z",
	                  "Annotations":{"127.0.0.1":{"Geo":{"postal_code":"10583"}},
	                                 "127.0.0.2":{"Geo":{"postal_code":"10584"}}}}`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, responseJSON)
	}))
	annotation.BatchURL = ts.URL
	defer ts.Close()

	for _, test := range tests {
		annotation.AddGeoAnnotations(test.ips, test.timestamp, test.geoDest)
		if diff := deep.Equal(test.geoDest, test.res); diff != nil {
			t.Error(diff)
		}
	}
}

func TestFetchAllAnnotations(t *testing.T) {
	tests := []struct {
		ips          []string
		requestTime  time.Time
		responseTime time.Time
		annMap       map[string]*api.GeoData
		res          []*api.GeoData
	}{
		{
			ips:          []string{"", "127.0.0.5", "2.2.2.2"},
			requestTime:  epoch,
			responseTime: time.Date(2018, 12, 05, 0, 0, 0, 0, time.UTC),
			annMap: map[string]*api.GeoData{
				"127.0.0.5": &api.GeoData{
					Geo:     &api.GeolocationIP{PostalCode: "10598"},
					Network: &api.ASData{Systems: []api.System{api.System{ASNs: []uint32{12345}}}}},
				"2.2.2.2": &api.GeoData{
					Geo:     &api.GeolocationIP{PostalCode: "10011"},
					Network: &api.ASData{Systems: []api.System{api.System{ASNs: []uint32{123, 456}}}}},
			},
			//Network":{"Systems":[{"ASNs":[123]}
			res: []*api.GeoData{
				nil,
				&api.GeoData{
					Geo:     &api.GeolocationIP{PostalCode: "10598"},
					Network: &api.ASData{Systems: []api.System{api.System{ASNs: []uint32{12345}}}}},
				&api.GeoData{
					Geo:     &api.GeolocationIP{PostalCode: "10011"},
					Network: &api.ASData{Systems: []api.System{api.System{ASNs: []uint32{123, 456}}}}},
			},
		},
	}
	var responseJSON string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, responseJSON)
	}))
	annotation.BatchURL = ts.URL
	defer ts.Close()

	for _, test := range tests {
		response := v2.Response{AnnotatorDate: test.responseTime, Annotations: test.annMap}
		//spew.Dump(response)
		responseBytes, _ := json.Marshal(response)
		responseJSON = string(responseBytes)
		ann := annotation.FetchAllAnnotations(test.ips, test.requestTime)
		if diff := deep.Equal(ann, test.res); diff != nil {
			t.Error(diff, spew.Sdump(ann), spew.Sdump(test.res))
		}
	}
}

func TestGetAndInsertGeolocationIPStruct(t *testing.T) {
	tests := []struct {
		geo       *api.GeolocationIP
		ip        string
		timestamp time.Time
		url       string
		res       *api.GeolocationIP
	}{
		{
			geo:       &api.GeolocationIP{},
			ip:        "123.123.123.001",
			timestamp: time.Now(),
			url:       "portGarbage",
			res:       &api.GeolocationIP{},
		},
		{
			geo: &api.GeolocationIP{},
			ip:  "127.0.0.1",
			url: "/10583",
			res: &api.GeolocationIP{PostalCode: "10583"},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"Geo":{"postal_code":"10583"},"ASN":{}}`)
	}))
	for _, test := range tests {
		annotation.BaseURL = ts.URL + test.url
		annotation.GetAndInsertGeolocationIPStruct(test.geo, test.ip, test.timestamp)
		if !reflect.DeepEqual(test.geo, test.res) {
			t.Errorf("Expected %v, got %v for test %s", test.res, test.geo, test.url)
		}
	}

}

func TestGetGeoData(t *testing.T) {
	tests := []struct {
		url string
		res *api.GeoData
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
			res: &api.GeoData{},
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
		res := annotation.GetGeoData(ts.URL + test.url)
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
		json, err := annotation.QueryAnnotationService(ts.URL + test.url)
		if err != nil && test.err == nil || err == nil && test.err != nil {
			t.Errorf("Expected %s, got %s, for %s", test.err, err, test.url)
		}
		if !bytes.Equal(json, test.res) {
			t.Errorf("Expected %s, got %s, for %s", string(test.res), string(json), test.url)
		}

	}
}

func TestParseJSONGeoDataResponse(t *testing.T) {
	tests := []struct {
		testBuffer  []byte
		resultData  *api.GeoData
		resultError error
	}{
		{
			testBuffer:  []byte(`{"Geo":null,"ASN":null}`),
			resultData:  &api.GeoData{Geo: nil, Network: nil},
			resultError: nil,
		},
		{
			testBuffer:  []byte(`"Geo":{},"ASN":{`),
			resultData:  nil,
			resultError: errors.New("Couldn't Parse JSON"),
		},
	}
	for _, test := range tests {
		res, err := annotation.ParseJSONGeoDataResponse(test.testBuffer)
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

func TestGetBatchGeoData(t *testing.T) {
	tests := []struct {
		url string
		res map[string]api.GeoData
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
			res: map[string]api.GeoData{"127.0.0.1xyz": {Geo: nil, Network: nil}},
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
		res := annotation.GetBatchGeoData(ts.URL+test.url, nil)
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
		json, err := annotation.BatchQueryAnnotationService(ts.URL+test.url, nil)
		if err != nil && test.err == nil || err == nil && test.err != nil {
			t.Errorf("Expected %s, got %s, for %s", test.err, err, test.url)
		}
		if !bytes.Equal(json, test.res) {
			t.Errorf("Expected %s, got %s, for %s", string(test.res), string(json), test.url)
		}

	}
}

func TestBatchParseJSONGeoDataResponse(t *testing.T) {
	tests := []struct {
		testBuffer  []byte
		resultData  map[string]api.GeoData
		resultError error
	}{
		{
			// Note: This is not testing for corruptedIP
			// addresses. The xyz could be a base36
			// encoded timestamp.
			testBuffer:  []byte(`{"127.0.0.1xyz": {"Geo":null,"ASN":null}}`),
			resultData:  map[string]api.GeoData{"127.0.0.1xyz": {Geo: nil, Network: nil}},
			resultError: nil,
		},
		{
			testBuffer:  []byte(`"Geo":{},"ASN":{`),
			resultData:  nil,
			resultError: errors.New("Couldn't Parse JSON"),
		},
	}
	for _, test := range tests {
		res, err := annotation.BatchParseJSONGeoDataResponse(test.testBuffer)
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
