package parser_test

// NOTE: Only the new V2 batch API is now tested.

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/go-test/deep"
	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/etl/annotation"
	p "github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
)

var epoch time.Time = time.Unix(0, 0)

func TestAddGeoDataPTConnSpec(t *testing.T) {
	tests := []struct {
		conspec   schema.MLabConnectionSpecification
		timestamp time.Time
		url       string
		res       schema.MLabConnectionSpecification
	}{
		{
			conspec:   schema.MLabConnectionSpecification{},
			timestamp: epoch,
			url:       "/notCalled",
			res:       schema.MLabConnectionSpecification{},
		},
		{
			conspec:   schema.MLabConnectionSpecification{Server_ip: "127.0.0.1"},
			timestamp: epoch,
			url:       "/src",
			res: schema.MLabConnectionSpecification{
				Server_ip:          "127.0.0.1",
				Server_geolocation: api.GeolocationIP{PostalCode: "10583"},
			},
		},
		{
			conspec:   schema.MLabConnectionSpecification{Client_ip: "127.0.0.1"},
			timestamp: epoch,
			url:       "/dest",
			res: schema.MLabConnectionSpecification{
				Client_ip:          "127.0.0.1",
				Client_geolocation: api.GeolocationIP{PostalCode: "10583"},
			},
		},
		{
			conspec:   schema.MLabConnectionSpecification{Server_ip: "127.0.0.1", Client_ip: "127.0.0.2"},
			timestamp: epoch,
			url:       "/both",
			res: schema.MLabConnectionSpecification{
				Server_ip:          "127.0.0.1",
				Server_geolocation: api.GeolocationIP{PostalCode: "10583"},
				Client_ip:          "127.0.0.2",
				Client_geolocation: api.GeolocationIP{PostalCode: "10584"},
			},
		},
	}
	responseJSON := `{"AnnotatorDate":"2018-12-05T00:00:00Z",
	                  "Annotations":{"127.0.0.1":{"Geo":{"postal_code":"10583"}},
	                                 "127.0.0.2":{"Geo":{"postal_code":"10584"}}}}`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, responseJSON)
	}))
	defer ts.Close()
	for _, test := range tests {
		annotation.BatchURL = ts.URL + test.url
		p.AddGeoDataPTConnSpec(&test.conspec, test.timestamp)
		if diff := deep.Equal(test.conspec, test.res); diff != nil {
			t.Error(test.url, diff)
		}
	}
}

// Test with the ::: bug.
func TestAddGeoDataPTHopBatchBadIPv6(t *testing.T) {
	tests := []struct {
		hops      []*schema.ParisTracerouteHop
		timestamp time.Time
		res       []*schema.ParisTracerouteHop
	}{
		{
			hops: []*schema.ParisTracerouteHop{
				&schema.ParisTracerouteHop{
					Src_ip:  "fe80:::301f:d5b0:3fb7:3a00",
					Dest_ip: "2620:0:1003:415:b33e:9d6a:81bf:87a1",
				},
			},
			timestamp: epoch,
			res: []*schema.ParisTracerouteHop{
				&schema.ParisTracerouteHop{
					Src_ip:           "fe80::301f:d5b0:3fb7:3a00",
					Src_geolocation:  api.GeolocationIP{AreaCode: 10583},
					Dest_ip:          "2620:0:1003:415:b33e:9d6a:81bf:87a1",
					Dest_geolocation: api.GeolocationIP{AreaCode: 10584},
				},
			},
		},
	}
	var body string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := ioutil.ReadAll(r.Body)
		body = string(b)
		fmt.Fprint(w, `{"fe80::301f:d5b0:3fb7:3a000" : {"Geo":{"area_code":10583},"ASN":{}}`+
			`,"2620:0:1003:415:b33e:9d6a:81bf:87a10" : {"Geo":{"area_code":10584},"ASN":{}}}`)
	}))
	defer ts.Close()
	for _, test := range tests {
		annotation.BatchURL = ts.URL + "?foobar"
		p.AddGeoDataPTHopBatch(test.hops, test.timestamp)
		if strings.Contains(body, ":::") {
			t.Errorf("Result contains :::")
		}
		if !reflect.DeepEqual(test.hops, test.res) {
			t.Errorf("Expected %v, got %v from data %s", *test.res[0], *test.hops[0], annotation.BatchURL)
		}
	}
}

func TestAddGeoDataPTHopBatch(t *testing.T) {
	tests := []struct {
		hops      []*schema.ParisTracerouteHop
		timestamp time.Time
		url       string
		res       []*schema.ParisTracerouteHop
	}{
		{
			hops: []*schema.ParisTracerouteHop{
				&schema.ParisTracerouteHop{
					Src_ip:  "127.0.0.1",
					Dest_ip: "1.0.0.127",
				},
			},
			timestamp: epoch,
			url:       "/10583?",
			res: []*schema.ParisTracerouteHop{
				&schema.ParisTracerouteHop{
					Src_ip:           "127.0.0.1",
					Src_geolocation:  api.GeolocationIP{AreaCode: 914},
					Dest_ip:          "1.0.0.127",
					Dest_geolocation: api.GeolocationIP{AreaCode: 212},
				},
			},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"127.0.0.10" : {"Geo":{"area_code":914},"ASN":{}}`+
			`,"1.0.0.1270" : {"Geo":{"area_code":212},"ASN":{}}}`)
	}))
	defer ts.Close()
	for _, test := range tests {
		annotation.BatchURL = ts.URL + test.url
		p.AddGeoDataPTHopBatch(test.hops, test.timestamp)
		if !reflect.DeepEqual(test.hops, test.res) {
			t.Errorf("Expected %v, got %v from data %s", test.res, test.hops, test.url)
		}
	}
}

func TestAnnotatePTHops(t *testing.T) {
	tests := []struct {
		hops           []*schema.ParisTracerouteHop
		annotationData map[string]api.GeoData
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
			annotationData: map[string]api.GeoData{},
			timestamp:      epoch,
			res:            []*schema.ParisTracerouteHop{nil},
		},
		{
			hops: []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Src_ip: "127.0.0.1"}},
			annotationData: map[string]api.GeoData{"127.0.0.10": api.GeoData{
				Geo: &api.GeolocationIP{}, Network: nil}},
			timestamp: epoch,
			res: []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Src_ip: "127.0.0.1",
				Src_geolocation: api.GeolocationIP{}}},
		},
		{
			hops: []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Dest_ip: "1.0.0.127"}},
			annotationData: map[string]api.GeoData{"1.0.0.1270": api.GeoData{
				Geo: &api.GeolocationIP{}, Network: nil}},
			timestamp: epoch,
			res: []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Dest_ip: "1.0.0.127",
				Dest_geolocation: api.GeolocationIP{}}},
		},
	}
	for _, test := range tests {
		p.AnnotatePTHops(test.hops, test.annotationData, test.timestamp)
		if !reflect.DeepEqual(test.hops, test.res) {
			t.Errorf("Expected %v, got %v.", test.res, test.hops)
		}
	}

}

func TestCreateRequestDataFromPTHops(t *testing.T) {
	tests := []struct {
		hops      []*schema.ParisTracerouteHop
		timestamp time.Time
		res       []api.RequestData
	}{
		{
			hops:      []*schema.ParisTracerouteHop{},
			timestamp: epoch,
			res:       []api.RequestData{},
		},
		{
			hops:      []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Dest_ip: "1.0.0.127"}},
			timestamp: epoch,
			res:       []api.RequestData{api.RequestData{"1.0.0.127", 0, epoch}},
		},
		{
			hops:      []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Src_ip: "127.0.0.1"}},
			timestamp: epoch,
			res:       []api.RequestData{api.RequestData{"127.0.0.1", 0, epoch}},
		},
	}
	for _, test := range tests {
		res := p.CreateRequestDataFromPTHops(test.hops, test.timestamp)
		if !reflect.DeepEqual(res, test.res) {
			t.Errorf("Expected %v, got %v.", test.res, res)
		}
	}
}

func TestAddGeoDataPTHop(t *testing.T) {
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
				Src_geolocation: api.GeolocationIP{PostalCode: "10583"},
			},
		},
		{
			hop:       schema.ParisTracerouteHop{Dest_ip: "127.0.0.1"},
			timestamp: time.Now(),
			url:       "/dest",
			res: schema.ParisTracerouteHop{
				Dest_ip:          "127.0.0.1",
				Dest_geolocation: api.GeolocationIP{PostalCode: "10583"},
			},
		},
		{
			hop:       schema.ParisTracerouteHop{Src_ip: "127.0.0.1", Dest_ip: "127.0.0.2"},
			timestamp: time.Now(),
			url:       "/both",
			res: schema.ParisTracerouteHop{
				Src_ip:           "127.0.0.1",
				Src_geolocation:  api.GeolocationIP{PostalCode: "10583"},
				Dest_ip:          "127.0.0.2",
				Dest_geolocation: api.GeolocationIP{PostalCode: "10583"},
			},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"Geo":{"postal_code":"10583"},"ASN":{}}`)
	}))
	defer ts.Close()
	for _, test := range tests {
		annotation.BaseURL = ts.URL + test.url
		p.AddGeoDataPTHop(&test.hop, test.timestamp)
		if !reflect.DeepEqual(test.hop, test.res) {
			t.Errorf("Expected %v, got %v for test %s", test.res, test.hop, test.url)
		}
	}
}

func testTime() time.Time {
	// Note: this timestamp corresponds to the "h3d0c0" in the result strings.
	tst, _ := time.Parse(time.RFC3339, "2002-10-02T15:00:00Z")
	return tst
}

func TestAddGeoDataNDTConnSpec(t *testing.T) {
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
		{ // This test exercises the error path one missing IP
			spec: func() schema.Web100ValueMap {
				spec := schema.EmptyConnectionSpec()
				spec["client_ip"] = "127.0.0.1"
				return spec
			}(),
			timestamp: testTime(),
			url:       "/10583?",
			res: func() schema.Web100ValueMap {
				spec := schema.EmptyConnectionSpec()
				spec["client_ip"] = "127.0.0.1"
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
				return spec
			}(),
		},
		{ // This test exercises the error path for missing IP addresses.
			spec: func() schema.Web100ValueMap {
				spec := schema.EmptyConnectionSpec()
				return spec
			}(),
			timestamp: testTime(),
			url:       "/10583?",
			res: func() schema.Web100ValueMap {
				spec := schema.EmptyConnectionSpec()
				return spec
			}(),
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"AnnotatorDate": "2018-12-05T00:00:00Z", "Annotations": {"127.0.0.1" : {"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10583,"postal_code":"10583","latitude":41.0051,"longitude":73.7846},"Network":{}}`+
			`,"1.0.0.127" : {"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10584,"postal_code":"10584","latitude":41.0051,"longitude":73.7846},"Network":{}}}}`)
	}))
	defer ts.Close()
	for _, test := range tests {
		annotation.BatchURL = ts.URL + test.url
		p.AddGeoDataNDTConnSpec(test.spec, test.timestamp)
		if diff := deep.Equal(test.spec, test.res); diff != nil {
			log.Println(test.spec)
			t.Error(test.spec, diff)
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
		if diff := deep.Equal(test.dest, test.res); diff != nil {
			t.Error(diff)
		}
	}

}

func TestGetAndInsertTwoSidedGeoIntoNDTConnSpec(t *testing.T) {
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

				cn := spec.Get("client").Get("network")
				cn["asn"] = int64(123)
				sn := spec.Get("server").Get("network")
				sn["asn"] = int64(456)

				return spec
			}(),
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"AnnotatorDate": "2018-12-05T00:00:00Z", "Annotations": {"127.0.0.1" : {"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10583,"postal_code":"10583","latitude":41.0051,"longitude":73.7846},"Network":{"Systems":[{"ASNs":[123]}]}}`+
			`,"1.0.0.127" : {"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10584,"postal_code":"10584","latitude":41.0051,"longitude":73.7846},"Network":{"Systems":[{"ASNs":[456]}]}}}}`)
	}))
	defer ts.Close()
	for _, test := range tests {
		annotation.BatchURL = ts.URL + test.url
		p.GetAndInsertTwoSidedGeoIntoNDTConnSpec(test.spec, test.timestamp)
		if diff := deep.Equal(test.spec, test.res); diff != nil {
			t.Error(test.url, diff)
		}
	}
}
