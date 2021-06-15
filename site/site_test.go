package site_test

import (
	"context"
	"flag"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/m-lab/etl/site"
	"github.com/m-lab/go/content"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/osx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/uuid-annotator/annotator"
)

var (
	localRawfile content.Provider
	corruptFile  content.Provider
	retiredFile  content.Provider
)

func setUp() {
	u, err := url.Parse("file:testdata/annotations.json")
	rtx.Must(err, "Could not parse URL")
	localRawfile, err = content.FromURL(context.Background(), u)
	rtx.Must(err, "Could not create content.Provider")

	u, err = url.Parse("file:testdata/corrupt-annotations.json")
	rtx.Must(err, "Could not parse URL")
	corruptFile, err = content.FromURL(context.Background(), u)
	rtx.Must(err, "Could not create content.Provider")

	u, err = url.Parse("file:testdata/retired-annotations.json")
	rtx.Must(err, "Could not parse URL")
	retiredFile, err = content.FromURL(context.Background(), u)
	rtx.Must(err, "Could not create content.Provider")
}

func TestBasic(t *testing.T) {
	setUp()
	ctx := context.Background()
	site.LoadFrom(ctx, localRawfile, retiredFile)
	missingServerAnn := annotator.ServerAnnotations{
		Machine: "foo",
		Site:    "bar",
		Geo: &annotator.Geolocation{
			Missing: true,
		},
		Network: &annotator.Network{
			Missing: true,
		},
	}

	defaultServerAnn := annotator.ServerAnnotations{
		Machine: "mlab1",
		Site:    "lga03",
		Geo: &annotator.Geolocation{
			ContinentCode: "NA",
			CountryCode:   "US",
			City:          "New York",
			Latitude:      40.7667,
			Longitude:     -73.8667,
		},
		Network: &annotator.Network{
			ASNumber: 6453,
			ASName:   "TATA COMMUNICATIONS (AMERICA) INC",
			Systems: []annotator.System{
				{ASNs: []uint32{6453}},
			},
		},
	}

	retiredServerann := annotator.ServerAnnotations{
		Machine: "mlab1",
		Site:    "acc01",
		Geo: &annotator.Geolocation{
			ContinentCode: "AF",
			CountryCode:   "GH",
			City:          "Accra",
			Latitude:      5.606,
			Longitude:     -0.1681,
		},
		Network: &annotator.Network{
			ASNumber: 30997,
			ASName:   "Ghana Internet Exchange Association",
			Systems: []annotator.System{
				{ASNs: []uint32{30997}},
			},
		},
	}

	tests := []struct {
		name          string
		site, machine string
		want          annotator.ServerAnnotations
	}{
		{
			name:    "success",
			site:    "lga03",
			machine: "mlab1",
			want:    defaultServerAnn,
		},
		{
			name:    "success-retired-site",
			site:    "acc01",
			machine: "mlab1",
			want:    retiredServerann,
		},
		{
			name:    "missing",
			site:    "bar",
			machine: "foo",
			want:    missingServerAnn,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ann := annotator.ServerAnnotations{}
			site.Annotate(tt.site, tt.machine, &ann)
			if diff := deep.Equal(ann, tt.want); diff != nil {
				t.Errorf("Annotate() failed; %s", strings.Join(diff, "\n"))
			}
		})
	}
}

func TestMustLoad(t *testing.T) {
	cleanupURL := osx.MustSetenv("SITEINFO_URL", "file:testdata/annotations.json")
	defer cleanupURL()
	cleanupRetiredURL := osx.MustSetenv("SITEINFO_RETIRED_URL", "file:testdata/retired-annotations.json")
	defer cleanupRetiredURL()
	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from environment variables")

	site.MustLoad(5 * time.Second)
}

func TestNilServer(t *testing.T) {
	setUp()
	ctx := context.Background()
	err := site.LoadFrom(ctx, localRawfile, retiredFile)
	if err != nil {
		t.Error(err)
	}
	// Should not panic!  Nothing else to check.
	site.Annotate("lga03", "mlab1", nil)
}

func TestCorrupt(t *testing.T) {
	setUp()
	ctx := context.Background()
	err := site.LoadFrom(ctx, corruptFile, corruptFile)
	if err == nil {
		t.Error("Expected load error")
	}
}
