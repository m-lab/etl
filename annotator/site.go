package uuid

import (
	"context"
	"encoding/json"
	"net"
	"sync"

	"github.com/m-lab/go/content"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/uuid-annotator/annotator"
)

// SiteAnnotator is the central struct for this module.
type SiteAnnotator struct {
	m              sync.RWMutex
	siteinfoSource content.Provider
	// Each site has a single ServerAnnotations struct, which
	// is later customized for each machine.
	sites map[string]annotator.ServerAnnotations
}

// New makes a new server Annotator using metadata from siteinfo JSON.
func New(ctx context.Context, js content.Provider) *SiteAnnotator {
	g := &SiteAnnotator{
		siteinfoSource: js,
	}
	var err error
	err = g.load(ctx)
	rtx.Must(err, "Could not load annotation db")
	return g
}

var missing = annotator.ServerAnnotations{
	Geo: &annotator.Geolocation{
		Missing: true,
	},
	Network: &annotator.Network{
		Missing: true,
	},
}

// Annotate adds site annotation for a site/machine
func (g *SiteAnnotator) Annotate(site, machine string, server *annotator.ServerAnnotations) {
	if server == nil {
		return
	}

	server.Machine = machine
	server.Site = site
	s, ok := g.sites[site]
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
func (g *SiteAnnotator) load(ctx context.Context) error {
	js, err := g.siteinfoSource.Get(ctx)
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
		g.sites[ann.Site] = ann.Annotation // Copy out of array.
	}
	return nil
}
