package schema

import (
	"flag"
	"io/ioutil"
	"log"
	"path"
	"reflect"
	"time"

	"github.com/m-lab/go/cloud/bqx"
)

// ParseInfo provides details about the parsed row. Uses 'Standard Column' names.
type ParseInfo struct {
	Version     string
	Time        time.Time
	ArchiveURL  string
	Filename    string
	Priority    int64
	GitCommit   string
	ArchiveSize int64
	FileSize    int64
}

// ServerInfo details various kinds of information about the server.
type ServerInfo struct {
	IP   string
	Port uint16
	IATA string

	Geo     *LegacyGeolocationIP
	Network *LegacyASData // NOTE: dominant ASN is available at top level.
}

// ClientInfo details various kinds of information about the client.
type ClientInfo struct {
	IP   string
	Port uint16

	Geo     *LegacyGeolocationIP
	Network *LegacyASData // NOTE: dominant ASN is available at top level.
}

// ParseInfoV0 provides details about the parsing of this row.
type ParseInfoV0 struct {
	TaskFileName  string // The tar file containing this test.
	ParseTime     time.Time
	ParserVersion string
	Filename      string
}

/*************************************************************************
*       DEPRECATED: Annotation Structs                         *
*************************************************************************/

// LegacyGeolocationIP preserves the schema for existing v1 datatype schemas. It should not be used for new datatypes.
// Deprecated: v1 annotation-service schema, preserved for backward compatibility. Do not reuse.
type LegacyGeolocationIP struct {
	ContinentCode       string  `json:"continent_code,,omitempty" bigquery:"continent_code"` // Gives a shorthand for the continent
	CountryCode         string  `json:"country_code,,omitempty"   bigquery:"country_code"`   // Gives a shorthand for the country
	CountryCode3        string  `json:"country_code3,,omitempty"  bigquery:"country_code3"`  // Gives a shorthand for the country
	CountryName         string  `json:"country_name,,omitempty"   bigquery:"country_name"`   // Name of the country
	Region              string  `json:"region,,omitempty"         bigquery:"region"`         // Region or State within the country
	Subdivision1ISOCode string  `json:",omitempty"`                                          // ISO3166-2 first-level country subdivision ISO code
	Subdivision1Name    string  `json:",omitempty"`                                          // ISO3166-2 first-level country subdivision name
	Subdivision2ISOCode string  `json:",omitempty"`                                          // ISO3166-2 second-level country subdivision ISO code
	Subdivision2Name    string  `json:",omitempty"`                                          // ISO3166-2 second-level country subdivision name
	MetroCode           int64   `json:"metro_code,,omitempty"     bigquery:"metro_code"`     // Metro code within the country
	City                string  `json:"city,,omitempty"           bigquery:"city"`           // City within the region
	AreaCode            int64   `json:"area_code,,omitempty"      bigquery:"area_code"`      // Area code, similar to metro code
	PostalCode          string  `json:"postal_code,,omitempty"    bigquery:"postal_code"`    // Postal code, again similar to metro
	Latitude            float64 `json:"latitude,,omitempty"       bigquery:"latitude"`       // Latitude
	Longitude           float64 `json:"longitude,,omitempty"      bigquery:"longitude"`      // Longitude
	AccuracyRadiusKm    int64   `json:"radius,,omitempty"         bigquery:"radius"`         // Accuracy Radius (geolite2 from 2018)

	Missing bool `json:",omitempty"` // True when the Geolocation data is missing from MaxMind.
}

type LegacySystem struct {
	// ASNs contains a single ASN, or AS set.  There must always be at least one ASN.
	// If there are more than one ASN, they are (arbitrarily) listed in increasing numerical order.
	ASNs []uint32
}

// LegacyASData preserves the schema for existing v1 datatype schemas. It should not be used for new datatypes.
// Deprecated: v1 annotation-service schema, preserved for backward compatibility. Do not reuse.
type LegacyASData struct {
	IPPrefix string `json:",omitempty"` // the IP prefix found in the table.
	CIDR     string `json:",omitempty"` // The IP prefix found in the RouteViews data.
	ASNumber uint32 `json:",omitempty"` // First AS number.
	ASName   string `json:",omitempty"` // AS name for that number, data from IPinfo.io
	Missing  bool   `json:",omitempty"` // True when the ASN data is missing from RouteViews.

	// One or more "Systems".  There must always be at least one System.  If there are more than one,
	// then this is a Multi-Origin AS, and the component Systems are in order of frequency in routing tables,
	// most common first.
	Systems []LegacySystem `json:",omitempty"`
}

// FindSchemaDocsFor should be used by parser row types to associate bigquery
// field descriptions with a schema generated from a row type.
func FindSchemaDocsFor(value interface{}) []bqx.SchemaDoc {
	docs := []bqx.SchemaDoc{}
	// Always include top level schema docs (should be common across row types).
	b, err := readAsset("toplevel.yaml")
	if err == nil {
		docs = append(docs, bqx.NewSchemaDoc(b))
	} else {
		log.Printf("WARNING: failed to read toplevel.yaml")
	}
	t := reflect.TypeOf(value)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	// Look for schema docs based on the given row type. Ignore missing schema docs.
	b, err = readAsset(t.Name() + ".yaml")
	if err == nil {
		docs = append(docs, bqx.NewSchemaDoc(b))
	} else {
		log.Printf("WARNING: no file for schema field description: %s.yaml", t.Name())
	}
	return docs
}

// assetDir provides a mechanism to override the embedded schema files.
var assetDir string

func init() {
	flag.StringVar(&assetDir, "schema.descriptions", "schema/descriptions",
		"Read description files from the given directory.")
}

func readAsset(name string) ([]byte, error) {
	return ioutil.ReadFile(path.Join(assetDir, name))
}
