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
	siteinfoRetired = flagx.URL{}
	globalAnnotator *annotator
)

func init() {
	flag.Var(&siteinfo, "siteinfo.url", "The URL for the Siteinfo JSON file containing server location and ASN metadata. gs:// and file:// schemes accepted.")
	flag.Var(&siteinfoRetired, "siteinfo.retired-url", "The URL for the Siteinfo retired JSON file. gs:// and file:// schemes accepted.")
	globalAnnotator = nil
}

// Annotate adds site annotation for a site/machine
func Annotate(site, machine string, server *uuid.ServerAnnotations) {
	if globalAnnotator != nil {
		globalAnnotator.Annotate(site, machine, server)
	}
}

// LoadFrom loads the site annotation source from the provider.
func LoadFrom(ctx context.Context, js content.Provider, retiredJS content.Provider) error {
	globalAnnotator = &annotator{
		siteinfoSource:        js,
		siteinfoRetiredSource: retiredJS,
		sites:                 make(map[string]uuid.ServerAnnotations, 200),
	}
	err := globalAnnotator.load(ctx)
	log.Println(len(globalAnnotator.sites), "sites loaded")
	return err
}

// MustLoad loads the site annotations source and will call log.Fatal if the
// loading fails.
func MustLoad(timeout time.Duration) {
	err := Load(timeout)
	rtx.Must(err, "Could not load annotation db")
}

// Load loads the site annotations source. Will try at least once, retry up to
// timeout and return an error if unsuccessful.
func Load(timeout time.Duration) error {
	js, err := content.FromURL(context.Background(), siteinfo.URL)
	rtx.Must(err, "Invalid server annotations URL", siteinfo.URL.String())

	retiredJS, err := content.FromURL(context.Background(), siteinfoRetired.URL)
	rtx.Must(err, "Invalid retired server annotations URL", siteinfoRetired.URL.String())

	// When annotations are read via HTTP, which is the default, a timeout of
	// 1 minute is used for the GET request.
	// The timeout specified here must be > 1 * time.Minute for the retry loop
	// to make sense.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for ; ctx.Err() == nil; time.Sleep(time.Second) {
		err = LoadFrom(context.Background(), js, retiredJS)
		if err == nil {
			break
		}
	}
	return err
}

// annotator stores the annotations, and provides Annotate method.
type annotator struct {
	siteinfoSource        content.Provider
	siteinfoRetiredSource content.Provider
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
	// Read the retired sites JSON file, and merge it with the current sites.
	retiredJS, err := sa.siteinfoRetiredSource.Get(ctx)
	if err != nil {
		return err
	}
	var retired []siteinfoAnnotation
	err = json.Unmarshal(retiredJS, &retired)
	if err != nil {
		return err
	}
	s = append(s, retired...)
	for _, ann := range s {
		// Machine should always be empty, filled in later.
		ann.Annotation.Machine = ""
		sa.sites[ann.Annotation.Site] = ann.Annotation // Copy out of array.
	}
	return nil
}
