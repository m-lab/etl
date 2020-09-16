package uuid

import (
	"context"
	"flag"
	"net"

	"github.com/m-lab/go/content"
	"github.com/m-lab/go/flagx"
	"github.com/m-lab/go/rtx"
	"github.com/m-lab/uuid-annotator/asnannotator"
	"github.com/m-lab/uuid-annotator/geoannotator"
	"github.com/m-lab/uuid-annotator/siteannotator"
)

var (
	maxmindurl  = flagx.URL{}
	routeviewv4 = flagx.URL{}
	routeviewv6 = flagx.URL{}
	asnameurl   = flagx.URL{}
	siteinfo    = flagx.URL{}
)

func findLocalIPs(localAddrs []net.Addr) []net.IP {
	localIPs := []net.IP{}
	for _, addr := range localAddrs {
		// By default, addr.String() includes the netblock suffix. By casting to
		// the underlying net.IPNet we can extract just the IP.
		if a, ok := addr.(*net.IPNet); ok {
			localIPs = append(localIPs, a.IP)
		}
	}
	return localIPs
}

func foo() {
	ctx := context.Background()

	flag.Parse()
	rtx.Must(flagx.ArgsFromEnv(flag.CommandLine), "Could not get args from environment variables")

	// Set up IP annotation, first by loading the initial config.
	localAddrs, err := net.InterfaceAddrs()
	rtx.Must(err, "Could not read local addresses")
	localIPs := findLocalIPs(localAddrs)
	p, err := content.FromURL(ctx, maxmindurl.URL)
	rtx.Must(err, "Could not get maxmind data from url")
	geo := geoannotator.New(ctx, p, localIPs)

	p4, err := content.FromURL(ctx, routeviewv4.URL)
	rtx.Must(err, "Could not load routeview v4 URL")
	p6, err := content.FromURL(ctx, routeviewv6.URL)
	rtx.Must(err, "Could not load routeview v6 URL")
	asnames, err := content.FromURL(ctx, asnameurl.URL)
	rtx.Must(err, "Could not load AS names URL")
	asn := asnannotator.New(ctx, p4, p6, asnames, localIPs)

	js, err := content.FromURL(ctx, siteinfo.URL)
	rtx.Must(err, "Could not load siteinfo URL")
	site := siteannotator.New(ctx, js)

}
