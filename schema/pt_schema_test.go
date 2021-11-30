package schema_test

import (
	"reflect"
	"testing"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/traceroute-caller/hopannotation"
	"github.com/m-lab/uuid-annotator/annotator"
)

func TestAnnotateHops(t *testing.T) {
	testMap := map[string]*api.Annotations{
		"91.213.30.229": {
			Geo: &api.GeolocationIP{
				ContinentCode: "NA",
				CountryCode:   "US",
				City:          "NYC",
				Latitude:      1.0,
				Longitude:     2.0,
			},
			Network: &api.ASData{
				ASNumber: 1234,
				Systems: []api.System{
					{ASNs: []uint32{1234}},
				},
			},
		},
		"91.169.126.135": {
			Geo: &api.GeolocationIP{
				ContinentCode: "EU",
				CountryCode:   "DE",
				Latitude:      3.0,
				Longitude:     4.0,
			},
			Network: &api.ASData{
				ASNumber: 4321,
				Systems: []api.System{
					{ASNs: []uint32{4321}},
				},
			},
		},
	}
	testHops := []schema.ScamperHop{{
		Source: schema.HopIP{
			IP:             "91.213.30.229",
			HopAnnotation1: &hopannotation.HopAnnotation1{},
		}}, {
		Source: schema.HopIP{
			IP: "91.169.126.135",
		}},
	}
	tests := []struct {
		name     string
		hops     []schema.ScamperHop
		annMap   map[string]*api.Annotations
		wantHops []schema.ScamperHop
	}{
		{
			name:     "empty-ann-map",
			hops:     testHops,
			annMap:   map[string]*api.Annotations{},
			wantHops: testHops,
		},
		{
			name:   "valid-input",
			hops:   testHops,
			annMap: testMap,
			wantHops: []schema.ScamperHop{{
				Source: schema.HopIP{
					IP:          "91.213.30.229",
					City:        "NYC",
					CountryCode: "US",
					ASN:         1234,
					HopAnnotation1: &hopannotation.HopAnnotation1{
						Annotations: &annotator.ClientAnnotations{
							Geo: &annotator.Geolocation{
								ContinentCode: "NA",
								CountryCode:   "US",
								City:          "NYC",
								Latitude:      1,
								Longitude:     2,
							},
							Network: &annotator.Network{
								ASNumber: 1234,
								Systems: []annotator.System{
									{ASNs: []uint32{uint32(1234)}},
								},
							},
						},
					},
				}}, {
				Source: schema.HopIP{
					IP:          "91.169.126.135",
					CountryCode: "DE",
					ASN:         4321,
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := schema.PTTest{}
			row.Hop = tt.hops
			annMap := tt.annMap
			row.AnnotateHops(annMap)

			if !reflect.DeepEqual(row.Hop, tt.wantHops) {
				t.Fatalf("failed to annotate hops,\nwanted: %+v\ngot: %+v", tt.wantHops, row.Hop)
			}
		})
	}
}
