// Package site provides site annotations.
package site

import (
	"context"
	"encoding/json"
	"flag"
	"net"

	"github.com/m-lab/go/content"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/uuid-annotator/annotator"
)

var (
	siteinfo        = flagx.URL{}
	globalAnnotator *siteAnnotator
)

func init() {
	flag.Var(&siteinfo, "siteinfo.url", "The URL for the Siteinfo JSON file containing server location and ASN metadata. gs:// and file:// schemes accepted.")
}

// Annotate adds site annotation for a site/machine
func Annotate(site, machine string, server *annotator.ServerAnnotations) {
	globalAnnotator.annotate(site, machine, server)
}

// LoadFrom loads the site annotation source from the provider.
func LoadFrom(ctx context.Context, js content.Provider) error {
	globalAnnotator = &siteAnnotator{
		siteinfoSource: js,
		sites:          make(map[string]annotator.ServerAnnotations, 200),
	}
	return globalAnnotator.load(ctx)
}

// MustLoad loads the site annotation source.
func MustLoad() {
	ctx := context.Background()

	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from environment variables")

	js, err := content.FromURL(ctx, siteinfo.URL)
	rtx.Must(err, "Could not load siteinfo URL")

	err = LoadFrom(ctx, js)
	rtx.Must(err, "Could not load annotation db")
}

// siteAnnotator is the central struct for this module.
type siteAnnotator struct {
	siteinfoSource content.Provider
	// Each site has a single ServerAnnotations struct, which
	// is later customized for each machine.
	sites map[string]annotator.ServerAnnotations
}

var missing = annotator.ServerAnnotations{
	Geo: &annotator.Geolocation{
		Missing: true,
	},
	Network: &annotator.Network{
		Missing: true,
	},
}

func (sa *siteAnnotator) annotate(site, machine string, server *annotator.ServerAnnotations) {
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

type siteinfoAnnotation struct {
	Site    string
	Network struct {
		IPv4 string
		IPv6 string
	}
	Annotation annotator.ServerAnnotations
}

// We may want this later to verify the network is consistent.
func parseCIDR(v4, v6 string) (net.IPNet, net.IPNet, error) {
	var v4ret, v6ret net.IPNet
	_, v4net, err := net.ParseCIDR(v4)
	if err != nil && v4 != "" {
		return v4ret, v6ret, err
	}
	if v4 != "" {
		v4ret = *v4net
	}
	_, v6net, err := net.ParseCIDR(v6)
	if err != nil && v6 != "" {
		return v4ret, v6ret, err
	}
	if v6 != "" {
		v6ret = *v6net
	}
	return v4ret, v6ret, nil
}

// load loads siteinfo dataset and returns them.
func (sa *siteAnnotator) load(ctx context.Context) error {
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
		sa.sites[ann.Site] = ann.Annotation // Copy out of array.
	}
	return nil
}
