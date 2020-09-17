package site_test

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"

	"github.com/go-test/deep"
	"github.com/m-lab/etl/site"
	"github.com/m-lab/go/content"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/tcp-info/inetdiag"
	"github.com/m-lab/uuid-annotator/annotator"
)

type badProvider struct {
	err error
}

func (b badProvider) Get(_ context.Context) ([]byte, error) {
	return nil, b.err
}

var (
	localRawfile content.Provider
	corruptFile  content.Provider
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
}

func TestBasic(t *testing.T) {
	setUp()
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

	tests := []struct {
		name          string
		site, machine string
		provider      *content.Provider
		want          annotator.ServerAnnotations
		wantErr       bool
	}{
		{
			name:     "success",
			site:     "lga03",
			machine:  "mlab1",
			provider: &localRawfile,
			want:     defaultServerAnn,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setUp()
			ctx := context.Background()
			site.LoadFrom(ctx, localRawfile)
			ann := annotator.ServerAnnotations{}
			site.Annotate(tt.site, tt.machine, &ann)
			if diff := deep.Equal(ann, tt.want); diff != nil {
				t.Errorf("Annotate() failed; %s", strings.Join(diff, "\n"))
			}
		})
	}
}

func Test_srvannotator_load(t *testing.T) {
	var bad content.Provider
	tests := []struct {
		name     string
		provider *content.Provider
		hostname string
		ID       *inetdiag.SockID
		want     *annotator.ServerAnnotations
		wantErr  bool
	}{
		{
			name:     "success",
			provider: &localRawfile,
			hostname: "mlab1.lga03.measurement-lab.org",
			want: &annotator.ServerAnnotations{
				Site:    "lga03",
				Machine: "mlab1",
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
			},
		},
		{
			name:     "success-project-flat-name",
			provider: &localRawfile,
			hostname: "mlab1-lga03.mlab-oti.measurement-lab.org",
			want: &annotator.ServerAnnotations{
				Site:    "lga03",
				Machine: "mlab1",
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
			},
		},
		{
			name:     "success-no-six",
			provider: &localRawfile,
			hostname: "mlab1.six01.measurement-lab.org",
			want: &annotator.ServerAnnotations{
				Site:    "six01",
				Machine: "mlab1",
				Geo: &annotator.Geolocation{
					City: "New York",
				},
				Network: &annotator.Network{
					ASName: "TATA COMMUNICATIONS (AMERICA) INC",
				},
			},
		},
		{
			name:     "error-bad-ipv4",
			provider: &localRawfile,
			hostname: "mlab1.bad04.measurement-lab.org",
			wantErr:  true,
		},
		{
			name:     "error-bad-ipv6",
			provider: &localRawfile,
			hostname: "mlab1.bad06.measurement-lab.org",
			wantErr:  true,
		},
		{
			name:     "error-loading-provider",
			provider: &bad,
			hostname: "mlab1.lga03.measurement-lab.org",
			wantErr:  true,
		},
		{
			name:     "error-corrupt-json",
			provider: &corruptFile,
			hostname: "mlab1.lga03.measurement-lab.org",
			wantErr:  true,
		},
		{
			name:     "error-bad-hostname",
			provider: &localRawfile,
			hostname: "this-is-not-a-hostname",
			wantErr:  true,
		},
		{
			name:     "error-bad-name-separator",
			provider: &localRawfile,
			hostname: "mlab1=lga03.mlab-oti.measurement-lab.org",
			wantErr:  true,
		},
		{
			name:     "error-hostname-not-in-annotations",
			provider: &localRawfile,
			hostname: "mlab1.abc01.measurement-lab.org",
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setUp()
			bad = &badProvider{fmt.Errorf("Fake load error")}
			ctx := context.Background()
			err := site.LoadFrom(ctx, *tt.provider)
			if (err != nil) != tt.wantErr {
				t.Errorf("srvannotator.Annotate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
