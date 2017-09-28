package parser_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/geo"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
)

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
		parser.CopyStructToMap(test.source, test.dest)
		if !reflect.DeepEqual(test.dest, test.res) {
			t.Errorf("Expected %+v, got %+v for data: %+v\n", test.res, test.dest, test.source)
		}
	}

}

func testTime() time.Time {
	tst, _ := time.Parse(time.RFC3339, "2002-10-02T15:00:00Z")
	return tst
}

var tests = []struct {
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
		timestamp: testTime(),
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
			geos["area_code"] = int64(10584)
			geos["postal_code"] = "10584"
			geos["latitude"] = float64(41.0051)
			geos["longitude"] = float64(73.7846)
			return spec
		}(),
	},
}

var callCount = 0

func TestDisabledAnnotation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount += 1
		fmt.Fprint(w, `{"127.0.0.1h3d0c0" : {"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10583,"postal_code":"10583","latitude":41.0051,"longitude":73.7846},"ASN":{}}`+
			`,"1.0.0.127h3d0c0" : {"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10584,"postal_code":"10584","latitude":41.0051,"longitude":73.7846},"ASN":{}}}`)
	}))
	for _, test := range tests {
		geo.BatchURL = ts.URL + test.url
		parser.AddMetaDataNDTConnSpec(test.spec, test.timestamp)
	}
	if callCount != 0 {
		t.Errorf("Annotator should not have been called.  Call count: %d", callCount)
	}
}

func TestAddMetaDataNDTConnSpec(t *testing.T) {
	parser.EnableAnnotation()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"127.0.0.1h3d0c0" : {"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10583,"postal_code":"10583","latitude":41.0051,"longitude":73.7846},"ASN":{}}`+
			`,"1.0.0.127h3d0c0" : {"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10584,"postal_code":"10584","latitude":41.0051,"longitude":73.7846},"ASN":{}}}`)
	}))
	for _, test := range tests {
		geo.BatchURL = ts.URL + test.url
		parser.AddMetaDataNDTConnSpec(test.spec, test.timestamp)
		if !reflect.DeepEqual(test.spec, test.res) {
			t.Errorf("Expected %+v, got %+v from data %s", test.res, test.spec, test.url)
		}
	}
}

func TestGetAndInsertTwoSidedMetaIntoNDTConnSpec(t *testing.T) {
	tst, _ := time.Parse(time.RFC3339, "2002-10-02T15:00:00Z")
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
			timestamp: tst,
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
				geos["area_code"] = int64(10584)
				geos["postal_code"] = "10584"
				geos["latitude"] = float64(41.0051)
				geos["longitude"] = float64(73.7846)
				return spec
			}(),
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"127.0.0.1h3d0c0" : {"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10583,"postal_code":"10583","latitude":41.0051,"longitude":73.7846},"ASN":{}}`+
			`,"1.0.0.127h3d0c0" : {"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10584,"postal_code":"10584","latitude":41.0051,"longitude":73.7846},"ASN":{}}}`)
	}))
	for _, test := range tests {
		geo.BatchURL = ts.URL + test.url
		parser.GetAndInsertTwoSidedMetaIntoNDTConnSpec(test.spec, test.timestamp)
		if !reflect.DeepEqual(test.spec, test.res) {
			t.Errorf("Expected %+v, got %+v from data %s", test.res, test.spec, test.url)
		}
	}
}
