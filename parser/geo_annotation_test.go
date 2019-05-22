package parser_test

// NOTE: Only the new V2 batch API is now tested.

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

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
			res: []api.RequestData{
				api.RequestData{IP: "1.0.0.127", IPFormat: 0, Timestamp: epoch}},
		},
		{
			hops:      []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Src_ip: "127.0.0.1"}},
			timestamp: epoch,
			res: []api.RequestData{
				api.RequestData{IP: "127.0.0.1", IPFormat: 0, Timestamp: epoch}},
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
