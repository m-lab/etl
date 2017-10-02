package annotation

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/m-lab/etl/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// TODO(gfr) Remove this comment after review.
// NOTE: the code was cut and pasted from parser version of file, to
// ensure no code was lost or duplicated.  Only minor additions and edits
// were made, which should be apparent in the diff.

var IPAnnotationEnabled = false

func init() {
	getFlagValues()
}

func getFlagValues() {
	// Check for ANNOTATE_IP = 'true'
	flag, ok := os.LookupEnv("ANNOTATE_IP")
	if ok {
		IPAnnotationEnabled, _ = strconv.ParseBool(flag)
		// If parse fails, then ipAnn will be set to false.
	}
}

// For testing.
func EnableAnnotation() {
	os.Setenv("ANNOTATE_IP", "True")
	getFlagValues()
}

// TODO(JosephMarques) See if there is a better way of determining
// where to send the request (there almost certainly is)
var AnnotatorURL = "https://annotator-dot-" +
	os.Getenv("GCLOUD_PROJECT") +
	".appspot.com"

var BaseURL = AnnotatorURL + "/annotate?"

var BatchURL = AnnotatorURL + "/batch_annotate"

// FetchGeoAnnotations takes a slice of strings
// containing ip addresses, a timestamp, and a slice of pointers to
// the GeolocationIP structs that correspond to the ip addresses. A
// precondition assumed by this function is that both slices are the
// same length. It will then make a call to the batch annotator, using
// the ip addresses and the timestamp. Then, it uses that data to fill
// in the structs pointed to by the slice of GeolocationIP pointers.
func FetchGeoAnnotations(ips []string, timestamp time.Time, geoDest []*GeolocationIP) {
	reqData := make([]RequestData, 0, len(ips))
	for _, ip := range ips {
		if ip == "" {
			// TODO(gfr) These should be warning, else we have error > request
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "Empty IP Address!!!"}).Inc()
			continue
		}
		reqData = append(reqData, RequestData{ip, 0, timestamp})
	}
	annotationData := GetBatchGeoData(BatchURL, reqData)
	timeString := strconv.FormatInt(timestamp.Unix(), 36)
	for index, ip := range ips {
		data, ok := annotationData[ip+timeString]
		if !ok || data.Geo == nil {
			// TODO(gfr) These should be warning, else we have error > request
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "Missing or empty data for IP Address!!!"}).Inc()
			continue
		}
		*geoDest[index] = *data.Geo

	}
}

// GetAndInsertGeolocationIPStruct takes a NON-NIL pointer to a
// pre-allocated GeolocationIP struct, an IP address, and a
// timestamp. It will connect to the annotation service, get the
// geo data, and insert the geo data into the reigion pointed to by
// the GeolocationIP pointer.
func GetAndInsertGeolocationIPStruct(geo *GeolocationIP, ip string, timestamp time.Time) {
	url := BaseURL + "ip_addr=" + url.QueryEscape(ip) +
		"&since_epoch=" + strconv.FormatInt(timestamp.Unix(), 10)
	annotationData := GetGeoData(url)
	if annotationData != nil && annotationData.Geo != nil {
		*geo = *annotationData.Geo
	}
}

// GetGeoData combines the functionality of QueryAnnotationService and
// ParseJSONGeoDataResponse to query the annotator service and return
// the corresponding GeoData if it can, or a nil pointer if it
// encounters any error and cannot get the data for any reason
func GetGeoData(url string) *GeoData {
	// Query the service and grab the response safely
	annotatorResponse, err := QueryAnnotationService(url)
	if err != nil {
		log.Println(err)
		return nil
	}

	// Safely parse the JSON response and pass it back to the caller
	geoDataFromResponse, err := ParseJSONGeoDataResponse(annotatorResponse)
	if err != nil {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "Failed to parse JSON"}).Inc()
		log.Println(err)
		return nil
	}
	return geoDataFromResponse
}

// QueryAnnotationService will connect to the annotation service and
// copy the body of a valid response to a byte slice and return it to a
// user, returning an error if any occurs
func QueryAnnotationService(url string) ([]byte, error) {
	metrics.AnnotationRequestCount.Inc()
	// Make the actual request
	resp, err := http.Get(url)

	// Catch http errors
	if err != nil {
		metrics.AnnotationErrorCount.
			With(prometheus.Labels{"source": "Request to Annotator failed"}).Inc()
		return nil, err
	}

	// Catch errors reported by the service
	if resp.StatusCode != http.StatusOK {
		metrics.AnnotationErrorCount.
			With(prometheus.Labels{"source": "Webserver gave non-ok response"}).Inc()
		return nil, errors.New("URL:" + url + " gave response code " + resp.Status)
	}
	defer resp.Body.Close()

	// Copy response into a byte slice
	return ioutil.ReadAll(resp.Body)
}

// ParseJSONGeoDataResponse takes a byte slice containing the test of
// the JSON from the annotator service and parses it into a GeoData
// struct, for easy manipulation. It returns a pointer to the struct on
// success and an error if an error occurs.
func ParseJSONGeoDataResponse(jsonBuffer []byte) (*GeoData, error) {
	parsedJSON := &GeoData{}
	err := json.Unmarshal(jsonBuffer, parsedJSON)
	if err != nil {
		return nil, err
	}
	return parsedJSON, nil
}

// GetBatchGeoData combines the functionality of
// BatchQueryAnnotationService and BatchParseJSONGeoDataResponse to
// query the annotator service and return the corresponding map of
// ip-timestamp strings to GeoData structs, or a nil map if it
// encounters any error and cannot get the data for any reason
// TODO - dedup common code in GetGeoData
func GetBatchGeoData(url string, data []RequestData) map[string]GeoData {
	// Query the service and grab the response safely
	annotatorResponse, err := BatchQueryAnnotationService(url, data)
	if err != nil {
		log.Println(err)
		return nil
	}

	// Safely parse the JSON response and pass it back to the caller
	geoDataFromResponse, err := BatchParseJSONGeoDataResponse(annotatorResponse)
	if err != nil {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "Failed to parse JSON"}).Inc()
		log.Println(err)
		return nil
	}
	return geoDataFromResponse
}

// BatchQueryAnnotationService takes a url to POST the request to and
// a slice of RequestDatas to be sent in the body in a JSON
// format. It will copy the response into a []byte and return it to
// the user, returning an error if any occurs
// TODO(gfr) Should pass the annotator's request context through and use it here.
func BatchQueryAnnotationService(url string, data []RequestData) ([]byte, error) {
	metrics.AnnotationRequestCount.Inc()

	encodedData, err := json.Marshal(data)
	if err != nil {
		metrics.AnnotationErrorCount.
			With(prometheus.Labels{"source": "Couldn't Marshal Data"}).Inc()
		return nil, err
	}

	var netClient = &http.Client{
		Timeout: time.Second,
	}

	// Make the actual request
	resp, err := netClient.Post(url, "raw", bytes.NewReader(encodedData))
	// Catch http errors
	if err != nil {
		metrics.AnnotationErrorCount.
			With(prometheus.Labels{"source": err.Error()}).Inc()
		return nil, err
	}

	// Catch errors reported by the service
	if resp.StatusCode != http.StatusOK {
		metrics.AnnotationErrorCount.
			With(prometheus.Labels{"source": http.StatusText(resp.StatusCode)}).Inc()
		return nil, errors.New("URL:" + url + " gave response code " + resp.Status)
	}
	defer resp.Body.Close()

	// Copy response into a byte slice
	return ioutil.ReadAll(resp.Body)
}

// BatchParseJSONGeoDataResponse takes a byte slice containing the
// text of the JSON from the annoator service's batch request endpoint
// and parses it into a map of strings to GeoData structs, for
// easy manipulation. It returns a pointer to the struct on success
// and an error if one occurs.
// TODO - is there duplicate code with ParseJSON... ?
func BatchParseJSONGeoDataResponse(jsonBuffer []byte) (map[string]GeoData, error) {
	parsedJSON := make(map[string]GeoData)
	err := json.Unmarshal(jsonBuffer, &parsedJSON)
	if err != nil {
		return nil, err
	}
	return parsedJSON, nil
}
