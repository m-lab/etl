package schema

// The GeolocationIP struct contains all the information needed for the
// geolocation data that will be inserted into big query. The fiels are
// capitalized for exporting, although the originals in the DB schema
// are not.
type GeolocationIP struct {
	Continent_code string  `json:"continent_code, string"` // Gives a shorthand for the continent
	Country_code   string  `json:"country_code, string"`   // Gives a shorthand for the country
	Country_code3  string  `json:"country_code3, string"`  // Gives a shorthand for the country
	Country_name   string  `json:"country_name, string"`   // Name of the country
	Region         string  `json:"region, string"`         // Region or State within the country
	Metro_code     int64   `json:"metro_code, integer"`    // Metro code within the country
	City           string  `json:"city, string"`           // City within the region
	Area_code      int64   `json:"area_code, integer"`     // Area code, similar to metro code
	Postal_code    string  `json:"postal_code, string"`    // Postal code, again similar to metro
	Latitude       float64 `json:"latitude, float"`        // Latitude
	Longitude      float64 `json:"longitude, float"`       // Longitude
}

// The struct that will hold the IP/ASN data when it gets added to the
// schema. Currently empty and unused.
type IPASNData struct{}

// The main struct for the metadata, which holds pointers to the
// Geolocation data and the IP/ASN data. This is what we parse the JSON
// response from the annotator into.
type MetaData struct {
	Geo *GeolocationIP // Holds the geolocation data
	ASN *IPASNData     // Holds the IP/ASN data
}
