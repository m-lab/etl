package parser_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/annotation"
	p "github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
)

var epoch time.Time = time.Unix(0, 0)

func TestAddMetaDataSSConnSpec(t *testing.T) {
	tests := []struct {
		conspec   schema.Web100ConnectionSpecification
		timestamp time.Time
		url       string
		res       schema.Web100ConnectionSpecification
	}{
		{
			conspec:   schema.Web100ConnectionSpecification{},
			timestamp: epoch,
			url:       "/notCalled",
			res:       schema.Web100ConnectionSpecification{},
		},
		{
			conspec:   schema.Web100ConnectionSpecification{Local_ip: "127.0.0.1"},
			timestamp: epoch,
			url:       "/src",
			res: schema.Web100ConnectionSpecification{
				Local_ip:          "127.0.0.1",
				Local_geolocation: annotation.GeolocationIP{Postal_code: "10583"},
			},
		},
		{
			conspec:   schema.Web100ConnectionSpecification{Remote_ip: "127.0.0.1"},
			timestamp: epoch,
			url:       "/dest",
			res: schema.Web100ConnectionSpecification{
				Remote_ip:          "127.0.0.1",
				Remote_geolocation: annotation.GeolocationIP{Postal_code: "10583"},
			},
		},
		{
			conspec:   schema.Web100ConnectionSpecification{Local_ip: "127.0.0.1", Remote_ip: "127.0.0.2"},
			timestamp: epoch,
			url:       "/both",
			res: schema.Web100ConnectionSpecification{
				Local_ip:           "127.0.0.1",
				Local_geolocation:  annotation.GeolocationIP{Postal_code: "10583"},
				Remote_ip:          "127.0.0.2",
				Remote_geolocation: annotation.GeolocationIP{Postal_code: "10584"},
			},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"127.0.0.10" : {"Geo":{"postal_code":"10583"},"ASN":{}}`+
			`,"127.0.0.20" : {"Geo":{"postal_code":"10584"},"ASN":{}}}`)
	}))
	for _, test := range tests {
		geo.BatchURL = ts.URL + test.url
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
				Server_geolocation: annotation.GeolocationIP{Postal_code: "10583"},
			},
		},
		{
			conspec:   schema.MLabConnectionSpecification{Client_ip: "127.0.0.1"},
			timestamp: epoch,
			url:       "/dest",
			res: schema.MLabConnectionSpecification{
				Client_ip:          "127.0.0.1",
				Client_geolocation: annotation.GeolocationIP{Postal_code: "10583"},
			},
		},
		{
			conspec:   schema.MLabConnectionSpecification{Server_ip: "127.0.0.1", Client_ip: "127.0.0.2"},
			timestamp: epoch,
			url:       "/both",
			res: schema.MLabConnectionSpecification{
				Server_ip:          "127.0.0.1",
				Server_geolocation: annotation.GeolocationIP{Postal_code: "10583"},
				Client_ip:          "127.0.0.2",
				Client_geolocation: annotation.GeolocationIP{Postal_code: "10584"},
			},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"127.0.0.10" : {"Geo":{"postal_code":"10583"},"ASN":{}}`+
			`,"127.0.0.20" : {"Geo":{"postal_code":"10584"},"ASN":{}}}`)
	}))
	for _, test := range tests {
		geo.BatchURL = ts.URL + test.url
		p.AddMetaDataPTConnSpec(&test.conspec, test.timestamp)
		if !reflect.DeepEqual(test.conspec, test.res) {
			t.Errorf("Expected %v, got %v for test %s", test.res, test.conspec, test.url)
		}
	}
}

func TestAddMetaDataPTHopBatch(t *testing.T) {
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
					Src_geolocation:  annotation.GeolocationIP{Area_code: 10583},
					Dest_ip:          "1.0.0.127",
					Dest_geolocation: annotation.GeolocationIP{Area_code: 10584},
				},
			},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"127.0.0.10" : {"Geo":{"area_code":10583},"ASN":{}}`+
			`,"1.0.0.1270" : {"Geo":{"area_code":10584},"ASN":{}}}`)
	}))
	for _, test := range tests {
		geo.BatchURL = ts.URL + test.url
		p.AddMetaDataPTHopBatch(test.hops, test.timestamp)
		if !reflect.DeepEqual(test.hops, test.res) {
			t.Errorf("Expected %s, got %s from data %s", test.res, test.hops, test.url)
		}
	}
}

func TestAnnotatePTHops(t *testing.T) {
	tests := []struct {
		hops           []*schema.ParisTracerouteHop
		annotationData map[string]annotation.MetaData
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
			annotationData: map[string]annotation.MetaData{},
			timestamp:      epoch,
			res:            []*schema.ParisTracerouteHop{nil},
		},
		{
			hops: []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Src_ip: "127.0.0.1"}},
			annotationData: map[string]annotation.MetaData{"127.0.0.10": annotation.MetaData{
				Geo: &annotation.GeolocationIP{}, ASN: nil}},
			timestamp: epoch,
			res: []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Src_ip: "127.0.0.1",
				Src_geolocation: annotation.GeolocationIP{}}},
		},
		{
			hops: []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Dest_ip: "1.0.0.127"}},
			annotationData: map[string]annotation.MetaData{"1.0.0.1270": annotation.MetaData{
				Geo: &annotation.GeolocationIP{}, ASN: nil}},
			timestamp: epoch,
			res: []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Dest_ip: "1.0.0.127",
				Dest_geolocation: annotation.GeolocationIP{}}},
		},
	}
	for _, test := range tests {
		p.AnnotatePTHops(test.hops, test.annotationData, test.timestamp)
		if !reflect.DeepEqual(test.hops, test.res) {
			t.Errorf("Expected %s, got %s.", test.res, test.hops)
		}
	}

}

func TestCreateRequestDataFromPTHops(t *testing.T) {
	tests := []struct {
		hops      []*schema.ParisTracerouteHop
		timestamp time.Time
		res       []annotation.RequestData
	}{
		{
			hops:      []*schema.ParisTracerouteHop{},
			timestamp: epoch,
			res:       []annotation.RequestData{},
		},
		{
			hops:      []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Dest_ip: "1.0.0.127"}},
			timestamp: epoch,
			res:       []annotation.RequestData{annotation.RequestData{"1.0.0.127", 0, epoch}},
		},
		{
			hops:      []*schema.ParisTracerouteHop{&schema.ParisTracerouteHop{Src_ip: "127.0.0.1"}},
			timestamp: epoch,
			res:       []annotation.RequestData{annotation.RequestData{"127.0.0.1", 0, epoch}},
		},
	}
	for _, test := range tests {
		res := p.CreateRequestDataFromPTHops(test.hops, test.timestamp)
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
				Src_geolocation: annotation.GeolocationIP{Postal_code: "10583"},
			},
		},
		{
			hop:       schema.ParisTracerouteHop{Dest_ip: "127.0.0.1"},
			timestamp: time.Now(),
			url:       "/dest",
			res: schema.ParisTracerouteHop{
				Dest_ip:          "127.0.0.1",
				Dest_geolocation: annotation.GeolocationIP{Postal_code: "10583"},
			},
		},
		{
			hop:       schema.ParisTracerouteHop{Src_ip: "127.0.0.1", Dest_ip: "127.0.0.2"},
			timestamp: time.Now(),
			url:       "/both",
			res: schema.ParisTracerouteHop{
				Src_ip:           "127.0.0.1",
				Src_geolocation:  annotation.GeolocationIP{Postal_code: "10583"},
				Dest_ip:          "127.0.0.2",
				Dest_geolocation: annotation.GeolocationIP{Postal_code: "10583"},
			},
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"Geo":{"postal_code":"10583"},"ASN":{}}`)
	}))
	for _, test := range tests {
		geo.BaseURL = ts.URL + test.url
		p.AddMetaDataPTHop(&test.hop, test.timestamp)
		if !reflect.DeepEqual(test.hop, test.res) {
			t.Errorf("Expected %v, got %v for test %s", test.res, test.hop, test.url)
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

func TestDisabledAnnotation(t *testing.T) {
	callCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount += 1
		fmt.Fprint(w, `{"127.0.0.1h3d0c0" : {"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10583,"postal_code":"10583","latitude":41.0051,"longitude":73.7846},"ASN":{}}`+
			`,"1.0.0.127h3d0c0" : {"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10584,"postal_code":"10584","latitude":41.0051,"longitude":73.7846},"ASN":{}}}`)
	}))
	for _, test := range tests {
		geo.BatchURL = ts.URL + test.url
		p.AddMetaDataNDTConnSpec(test.spec, test.timestamp)
	}
	if callCount != 0 {
		t.Errorf("Annotator should not have been called.  Call count: %d", callCount)
	}
}

func TestAddMetaDataNDTConnSpec(t *testing.T) {
	geo.EnableAnnotation()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"127.0.0.1h3d0c0" : {"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10583,"postal_code":"10583","latitude":41.0051,"longitude":73.7846},"ASN":{}}`+
			`,"1.0.0.127h3d0c0" : {"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10584,"postal_code":"10584","latitude":41.0051,"longitude":73.7846},"ASN":{}}}`)
	}))
	for _, test := range tests {
		geo.BatchURL = ts.URL + test.url
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
		fmt.Fprint(w, `{"Geo":{"continent_code":"","country_code":"US","country_code3":"USA","country_name":"United States of America","region":"NY","metro_code":0,"city":"Scarsdale","area_code":10583,"postal_code":"10583","latitude":41.0051,"longitude":73.7846},"ASN":{}}`)
	}))
	for _, test := range tests {
		geo.BaseURL = ts.URL + test.url
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
		p.GetAndInsertTwoSidedMetaIntoNDTConnSpec(test.spec, test.timestamp)
		if !reflect.DeepEqual(test.spec, test.res) {
			t.Errorf("Expected %+v, got %+v from data %s", test.res, test.spec, test.url)
		}
	}
}
