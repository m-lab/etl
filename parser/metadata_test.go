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
				geoc["continent_code"] = ""
				geoc["country_code"] = "US"
				geoc["country_code3"] = "USA"
				geoc["country_name"] = "United States of America"
				geoc["region"] = "NY"
				geoc["metro_code"] = int64(0)
				geoc["city"] = "Scarsdale"
				geoc["area_code"] = int64(10583)
				geoc["postal_code"] = "10583"
				geoc["latitude"] = float64(0.0)
				geoc["longitude"] = float64(0.0)
				geos := spec.Get("server_geolocation")
				geos["continent_code"] = ""
				geos["country_code"] = "US"
				geos["country_code3"] = "USA"
				geos["country_name"] = "United States of America"
				geos["region"] = "NY"
				geos["metro_code"] = int64(0)
				geos["city"] = "Scarsdale"
				geos["area_code"] = int64(10583)
				geos["postal_code"] = "10583"
				geos["latitude"] = float64(0.0)
				geos["longitude"] = float64(0.0)
				return spec
			}(),
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10583,"postal_code":"10583","latitude":0,"longitude":0},"ASN":{}}`)
	}))
	for _, test := range tests {
		p.BaseURL = ts.URL + test.url
		p.AddMetaDataNDTConnSpec(test.spec, test.timestamp)
		if !reflect.DeepEqual(test.spec, test.res) {
			t.Errorf("Expected %+v, got %+v from data %s", test.res, test.spec, test.url)
		}
	}
}

func TestGetAndInsertNDT(t *testing.T) {
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
				geo["continent_code"] = ""
				geo["country_code"] = "US"
				geo["country_code3"] = "USA"
				geo["country_name"] = "United States of America"
				geo["region"] = "NY"
				geo["metro_code"] = int64(0)
				geo["city"] = "Scarsdale"
				geo["area_code"] = int64(10583)
				geo["postal_code"] = "10583"
				geo["latitude"] = float64(0.0)
				geo["longitude"] = float64(0.0)
				return spec
			}(),
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10583,"postal_code":"10583","latitude":0,"longitude":0},"ASN":{}}`)
	}))
	for _, test := range tests {
		p.BaseURL = ts.URL + test.url
		p.GetAndInsertNDT(test.side, test.spec, test.timestamp)
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
				A   int
				Bee string
			}{A: 1, Bee: "2"},
			dest: make(map[string]bigquery.Value),
			res:  map[string]bigquery.Value{"a": 1, "bee": "2"},
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
		if err == nil && test.resultError != nil ||
			err != nil && test.resultError == nil {
			t.Errorf("Expected %s, got %s for data: %s\n", test.resultError, err, string(test.testBuffer))
		} else if !reflect.DeepEqual(res, test.resultData) {
			t.Errorf("Expected %+v, got %+v, for data %s\n", test.resultData, res, string(test.testBuffer))
		}
	}
}
