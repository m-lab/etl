// Package site provides site annotations.
package site

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"time"

	"github.com/m-lab/go/content"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"
	uuid "github.com/m-lab/uuid-annotator/annotator"
)

var (
	// For example of how siteinfo is loaded on production servers, see
	// https://github.com/m-lab/k8s-support/blob/ff5b53faef7828d11d45c2a4f27d53077ddd080c/k8s/daemonsets/templates.jsonnet#L350
	siteinfo        = flagx.URL{}
	globalAnnotator *annotator
)

func init() {
	flag.Var(&siteinfo, "siteinfo.url", "The URL for the Siteinfo JSON file containing server location and ASN metadata. gs:// and file:// schemes accepted.")
	globalAnnotator = nil
}

// Annotate adds site annotation for a site/machine
func Annotate(site, machine string, server *uuid.ServerAnnotations) {
	if globalAnnotator != nil {
		globalAnnotator.Annotate(site, machine, server)
	}
}

// LoadFrom loads the site annotation source from the provider.
func LoadFrom(ctx context.Context, js content.Provider) error {
	globalAnnotator = &annotator{
		siteinfoSource: js,
		sites:          make(map[string]uuid.ServerAnnotations, 200),
	}
	err := globalAnnotator.load(ctx)
	log.Println(len(globalAnnotator.sites), "sites loaded")
	return err
}

// MustLoad loads the site annotation source.  Will try at least once,
// and retry for up to timeout
func MustLoad(timeout time.Duration) {
	start := time.Now()
	ctx := context.Background()
	// Retry for up to 30 seconds
	var js content.Provider
	var err error
	for ; time.Since(start) < timeout; time.Sleep(time.Second) {
		js, err = content.FromURL(ctx, siteinfo.URL)
		if err == nil {
			break
		}
	}
	rtx.Must(err, "Could not load siteinfo URL")

	for ; time.Since(start) < timeout; time.Sleep(time.Second) {
		err = LoadFrom(ctx, js)
		if err == nil {
			break
		}
	}
	rtx.Must(err, "Could not load annotation db")

}

// annotator stores the annotations, and provides Annotate method.
type annotator struct {
	siteinfoSource content.Provider
	// Each site has a single ServerAnnotations struct, which
	// is later customized for each machine.
	sites map[string]uuid.ServerAnnotations
}

// missing is used if annotation is requested for a non-existant server.
var missing = uuid.ServerAnnotations{
	Geo: &uuid.Geolocation{
		Missing: true,
	},
	Network: &uuid.Network{
		Missing: true,
	},
}

// Annotate annotates the server with the approprate annotations.
func (sa *annotator) Annotate(site, machine string, server *uuid.ServerAnnotations) {
	if server == nil {
		return
	}

	server.Machine = machine
	server.Site = site
	s, ok := sa.sites[site]
	if !ok {
		server.Geo = missing.Geo
		server.Network = missing.Network
		return
	}
	server.Geo = s.Geo
	server.Network = s.Network
}

// load loads siteinfo dataset and returns them.
func (sa *annotator) load(ctx context.Context) error {
	// siteinfoAnnotation struct is used for parsing the json annotation source.
	type siteinfoAnnotation struct {
		Site    string
		Network struct {
			IPv4 string
			IPv6 string
		}
		Annotation uuid.ServerAnnotations
	}

	js, err := sa.siteinfoSource.Get(ctx)
	if err != nil {
		return err
	}
	var s []siteinfoAnnotation
	err = json.Unmarshal(js, &s)
	if err != nil {
		return err
	}
	for _, ann := range s {
		// Machine should always be empty, filled in later.
		ann.Annotation.Machine = ""
		sa.sites[ann.Annotation.Site] = ann.Annotation // Copy out of array.
	}
	return nil
}