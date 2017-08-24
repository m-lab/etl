package parser_test

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

	"cloud.google.com/go/bigquery"

	p "github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
)

func TestAddMetaDataSSConnSpec(t *testing.T) {
	tests := []struct {
		conspec   schema.Web100ConnectionSpecification
		timestamp time.Time
		url       string
		res       schema.Web100ConnectionSpecification
	}{
		{
			conspec:   schema.Web100ConnectionSpecification{},
			timestamp: time.Now(),
			url:       "/notCalled",
			res:       schema.Web100ConnectionSpecification{},
		},
		{
			conspec:   schema.Web100ConnectionSpecification{Local_ip: "127.0.0.1"},
			timestamp: time.Now(),
			url:       "/src",
			res: schema.Web100ConnectionSpecification{
				Local_ip:          "127.0.0.1",
				Local_geolocation: schema.GeolocationIP{Postal_code: "10583"},
			},
		},
		{
			conspec:   schema.Web100ConnectionSpecification{Remote_ip: "127.0.0.1"},
			timestamp: time.Now(),
			url:       "/dest",
			res: schema.Web100ConnectionSpecification{
				Remote_ip:          "127.0.0.1",
				Remote_geolocation: schema.GeolocationIP{Postal_code: "10583"},
			},
		},
		{
			conspec:   schema.Web100ConnectionSpecification{Local_ip: "127.0.0.1", Remote_ip: "127.0.0.2"},
			timestamp: time.Now(),
			url:       "/both",
			res: schema.Web100ConnectionSpecification{
				Local_ip:           "127.0.0.1",
				Local_geolocation:  schema.GeolocationIP{Postal_code: "10583"},
				Remote_ip:          "127.0.0.2",
				Remote_geolocation: schema.GeolocationIP{Postal_code: "10583"},
			},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"Geo":{"postal_code":"10583"},"ASN":{}}`)
	}))
	for _, test := range tests {
		p.BaseURL = ts.URL + test.url
		p.AddMetaDataSSConnSpec(&test.conspec, test.timestamp)
		if !reflect.DeepEqual(test.conspec, test.res) {
			t.Errorf("Expected %v, got %v for test %s", test.res, test.conspec, test.url)
		}
	}
}

func TestAddMetaDataPTConnSpec(t *testing.T) {
	tests := []struct {
		conspec   schema.MLabConnectionSpecification
		timestamp time.Time
		url       string
		res       schema.MLabConnectionSpecification
	}{
		{
			conspec:   schema.MLabConnectionSpecification{},
			timestamp: time.Now(),
			url:       "/notCalled",
			res:       schema.MLabConnectionSpecification{},
		},
		{
			conspec:   schema.MLabConnectionSpecification{Server_ip: "127.0.0.1"},
			timestamp: time.Now(),
			url:       "/src",
			res: schema.MLabConnectionSpecification{
				Server_ip:          "127.0.0.1",
				Server_geolocation: schema.GeolocationIP{Postal_code: "10583"},
			},
		},
		{
			conspec:   schema.MLabConnectionSpecification{Client_ip: "127.0.0.1"},
			timestamp: time.Now(),
			url:       "/dest",
			res: schema.MLabConnectionSpecification{
				Client_ip:          "127.0.0.1",
				Client_geolocation: schema.GeolocationIP{Postal_code: "10583"},
			},
		},
		{
			conspec:   schema.MLabConnectionSpecification{Server_ip: "127.0.0.1", Client_ip: "127.0.0.2"},
			timestamp: time.Now(),
			url:       "/both",
			res: schema.MLabConnectionSpecification{
				Server_ip:          "127.0.0.1",
				Server_geolocation: schema.GeolocationIP{Postal_code: "10583"},
				Client_ip:          "127.0.0.2",
				Client_geolocation: schema.GeolocationIP{Postal_code: "10583"},
			},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"Geo":{"postal_code":"10583"},"ASN":{}}`)
	}))
	for _, test := range tests {
		p.BaseURL = ts.URL + test.url
		p.AddMetaDataPTConnSpec(&test.conspec, test.timestamp)
		if !reflect.DeepEqual(test.conspec, test.res) {
			t.Errorf("Expected %v, got %v for test %s", test.res, test.conspec, test.url)
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
				Src_geolocation: schema.GeolocationIP{Postal_code: "10583"},
			},
		},
		{
			hop:       schema.ParisTracerouteHop{Dest_ip: "127.0.0.1"},
			timestamp: time.Now(),
			url:       "/dest",
			res: schema.ParisTracerouteHop{
				Dest_ip:          "127.0.0.1",
				Dest_geolocation: schema.GeolocationIP{Postal_code: "10583"},
			},
		},
		{
			hop:       schema.ParisTracerouteHop{Src_ip: "127.0.0.1", Dest_ip: "127.0.0.2"},
			timestamp: time.Now(),
			url:       "/both",
			res: schema.ParisTracerouteHop{
				Src_ip:           "127.0.0.1",
				Src_geolocation:  schema.GeolocationIP{Postal_code: "10583"},
				Dest_ip:          "127.0.0.2",
				Dest_geolocation: schema.GeolocationIP{Postal_code: "10583"},
			},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"Geo":{"postal_code":"10583"},"ASN":{}}`)
	}))
	for _, test := range tests {
		p.BaseURL = ts.URL + test.url
		p.AddMetaDataPTHop(&test.hop, test.timestamp)
		if !reflect.DeepEqual(test.hop, test.res) {
			t.Errorf("Expected %v, got %v for test %s", test.res, test.hop, test.url)
		}
	}
}

func TestGetAndInsertGeolocationIPStruct(t *testing.T) {
	tests := []struct {
		geo       *schema.GeolocationIP
		ip        string
		timestamp time.Time
		url       string
		res       *schema.GeolocationIP
	}{
		{
			geo:       &schema.GeolocationIP{},
			ip:        "123.123.123.001",
			timestamp: time.Now(),
			url:       "portGarbage",
			res:       &schema.GeolocationIP{},
		},
		{
			geo: &schema.GeolocationIP{},
			ip:  "127.0.0.1",
			url: "/10583",
			res: &schema.GeolocationIP{Postal_code: "10583"},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"Geo":{"postal_code":"10583"},"ASN":{}}`)
	}))
	for _, test := range tests {
		p.BaseURL = ts.URL + test.url
		p.GetAndInsertGeolocationIPStruct(test.geo, test.ip, test.timestamp)
		if !reflect.DeepEqual(test.geo, test.res) {
			t.Errorf("Expected %v, got %v for test %s", test.res, test.geo, test.url)
		}
	}

}

func TestAddMetaDataNDTConnSpec(t *testing.T) {
	tests := []struct {
		spec      schema.Web100ValueMap
		timestamp time.Time
		url       string
		res       schema.Web100ValueMap
	}{
		{
			spec: func() schema.Web100ValueMap {
				spec := schema.EmptyConnectionSpec()
				spec["client_ip"] = "127.0.0.1"
				spec["server_ip"] = "1.0.0.127"
				return spec
			}(),
			timestamp: time.Now(),
			url:       "/10583?",
			res: func() schema.Web100ValueMap {
				spec := schema.EmptyConnectionSpec()
				spec["client_ip"] = "127.0.0.1"
				spec["server_ip"] = "1.0.0.127"
				geoc := spec.Get("client_geolocation")
				geoc["country_code"] = "US"
				geoc["country_code3"] = "USA"
				geoc["country_name"] = "United States of America"
				geoc["region"] = "NY"
				geoc["city"] = "Scarsdale"
				geoc["area_code"] = int64(10583)
				geoc["postal_code"] = "10583"
				geoc["latitude"] = float64(41.0051)
				geoc["longitude"] = float64(73.7846)
				geos := spec.Get("server_geolocation")
				geos["country_code"] = "US"
				geos["country_code3"] = "USA"
				geos["country_name"] = "United States of America"
				geos["region"] = "NY"
				geos["city"] = "Scarsdale"
				geos["area_code"] = int64(10583)
				geos["postal_code"] = "10583"
				geos["latitude"] = float64(41.0051)
				geos["longitude"] = float64(73.7846)
				return spec
			}(),
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10583,"postal_code":"10583","latitude":41.0051,"longitude":73.7846},"ASN":{}}`)
	}))
	for _, test := range tests {
		p.BaseURL = ts.URL + test.url
		p.AddMetaDataNDTConnSpec(test.spec, test.timestamp)
		if !reflect.DeepEqual(test.spec, test.res) {
			t.Errorf("Expected %+v, got %+v from data %s", test.res, test.spec, test.url)
		}
	}
}

func TestGetAndInsertMetaIntoNDTConnSpec(t *testing.T) {
	tests := []struct {
		side      string
		spec      schema.Web100ValueMap
		timestamp time.Time
		url       string
		res       schema.Web100ValueMap
	}{
		{
			side:      "broken",
			spec:      schema.EmptyConnectionSpec(),
			timestamp: time.Now(),
			url:       "/notUsed?",
			res:       schema.EmptyConnectionSpec(),
		},
		{
			side:      "client",
			spec:      schema.EmptyConnectionSpec(),
			timestamp: time.Now(),
			url:       "portGarbage",
			res:       schema.EmptyConnectionSpec(),
		},
		{
			side: "client",
			spec: func() schema.Web100ValueMap {
				spec := schema.EmptyConnectionSpec()
				spec["client_ip"] = "127.0.0.1"
				return spec
			}(),
			timestamp: time.Now(),
			url:       "/10583?",
			res: func() schema.Web100ValueMap {
				spec := schema.EmptyConnectionSpec()
				spec["client_ip"] = "127.0.0.1"
				geo := spec.Get("client_geolocation")
				geo["country_code"] = "US"
				geo["country_code3"] = "USA"
				geo["country_name"] = "United States of America"
				geo["region"] = "NY"
				geo["city"] = "Scarsdale"
				geo["area_code"] = int64(10583)
				geo["postal_code"] = "10583"
				geo["latitude"] = float64(41.0051)
				geo["longitude"] = float64(73.7846)
				return spec
			}(),
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10583,"postal_code":"10583","latitude":0,"longitude":0},"ASN":{}}`)
	}))
	for _, test := range tests {
		p.BaseURL = ts.URL + test.url
		p.GetAndInsertMetaIntoNDTConnSpec(test.side, test.spec, test.timestamp)
		if !reflect.DeepEqual(test.spec, test.res) {
			t.Errorf("Expected %+v, got %+v from data %s", test.res, test.spec, test.url)
		}
	}

}

func TestCopyStructToMap(t *testing.T) {
	tests := []struct {
		source interface{}
		dest   map[string]bigquery.Value
		res    map[string]bigquery.Value
	}{
		{
			source: &struct {
				A   int64
				Bee string
			}{A: 1, Bee: "2"},
			dest: make(map[string]bigquery.Value),
			res:  map[string]bigquery.Value{"a": int64(1), "bee": "2"},
		},
		{
			source: &struct {
				A   int64
				Bee string
			}{A: 0, Bee: ""},
			dest: make(map[string]bigquery.Value),
			res:  map[string]bigquery.Value{},
		},
		{
			source: &struct{}{},
			dest:   make(map[string]bigquery.Value),
			res:    map[string]bigquery.Value{},
		},
	}
	for _, test := range tests {
		p.CopyStructToMap(test.source, test.dest)
		if !reflect.DeepEqual(test.dest, test.res) {
			t.Errorf("Expected %+v, got %+v for data: %+v\n", test.res, test.dest, test.source)
		}
	}

}

func TestGetMetaData(t *testing.T) {
	tests := []struct {
		url string
		res *schema.MetaData
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
			res: &schema.MetaData{},
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
		res := p.GetMetaData(ts.URL + test.url)
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
		json, err := p.QueryAnnotationService(ts.URL + test.url)
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
		resultData  *schema.MetaData
		resultError error
	}{
		{
			testBuffer:  []byte(`{"Geo":null,"ASN":null}`),
			resultData:  &schema.MetaData{Geo: nil, ASN: nil},
			resultError: nil,
		},
		{
			testBuffer:  []byte(`"Geo":{},"ASN":{`),
			resultData:  nil,
			resultError: errors.New("Couldn't Parse JSON"),
		},
	}
	for _, test := range tests {
		res, err := p.ParseJSONMetaDataResponse(test.testBuffer)
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
