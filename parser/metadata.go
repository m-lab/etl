package parser

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
	"github.com/prometheus/client_golang/prometheus"
)

// TODO(JosephMarques) See if there is a better way of determining
// where to send the request (there almost certainly is)
var BaseURL = "https://annotator-dot-" +
	os.Getenv("GCLOUD_PROJECT") +
	".appspot.com/annotate?"

func AddMetaDataPTConnSpec(spec *schema.MLabConnectionSpecification, timestamp time.Time) {
	// Time the response
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.AnnotationTimeSummary.
			With(prometheus.Labels{"test_type": "PT"}).
			Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)
	if spec.Server_ip != "" {
		GetAndInsertGeolocationIPStruct(&spec.Server_geolocation, spec.Server_ip, timestamp)
	}
	if spec.Client_ip != "" {
		GetAndInsertGeolocationIPStruct(&spec.Client_geolocation, spec.Client_ip, timestamp)
	}
}

func AddMetaDataPTHop(hop *schema.ParisTracerouteHop, timestamp time.Time) {
	// Time the response
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.AnnotationTimeSummary.
			With(prometheus.Labels{"test_type": "PT-HOP"}).
			Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)
	if hop.Src_ip != "" {
		GetAndInsertGeolocationIPStruct(&hop.Src_geolocation, hop.Src_ip, timestamp)
	}
	if hop.Dest_ip != "" {
		GetAndInsertGeolocationIPStruct(&hop.Dest_geolocation, hop.Dest_ip, timestamp)
	}
}

func GetAndInsertGeolocationIPStruct(geo *schema.GeolocationIP, ip string, timestamp time.Time) {
	url := BaseURL + "ip_addr=" + url.QueryEscape(ip) +
		"&since_epoch=" + strconv.FormatInt(timestamp.Unix(), 10)
	annotationData := GetMetaData(url)
	if annotationData != nil && annotationData.Geo != nil {
		*geo = *annotationData.Geo
	}
}

// AddMetaDataNDTConnSpec takes a connection spec and a timestamp and
// annotates the connection spec with metadata associated with each IP
// Address. It will either sucessfully add the metadata or fail
// silently and make no changes.
func AddMetaDataNDTConnSpec(spec schema.Web100ValueMap, timestamp time.Time) {
	// Time the response
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.AnnotationTimeSummary.
			With(prometheus.Labels{"test_type": "NDT"}).
			Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)

	GetAndInsertMetaIntoNDTConnSpec("client", spec, timestamp)
	GetAndInsertMetaIntoNDTConnSpec("server", spec, timestamp)
}

// GetAndInsertNDT takes a timestamp, an NDT connection spec, and a
// string indicating whether it should get the metadata for the client
// end or the server end of the connection. It will either insert the
// data into the connection spec or silently fail.
func GetAndInsertMetaIntoNDTConnSpec(side string, spec schema.Web100ValueMap, timestamp time.Time) {
	ip, ok := spec.GetString([]string{side + "_ip"})
	if ok {
		url := BaseURL + "ip_addr=" + url.QueryEscape(ip) +
			"&since_epoch=" + strconv.FormatInt(timestamp.Unix(), 10)
		annotationData := GetMetaData(url)
		if annotationData != nil && annotationData.Geo != nil {
			CopyStructToMap(annotationData.Geo, spec.Get(side+"_geolocation"))
		} else {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "Couldn't get metadata for the " + side + " side."}).Inc()
		}
	}
}

// CopyStructToMap takes a POINTER to an arbitrary struct and copies
// it's fields into a value map. It will also make fields entirely
// lower case, for convienece when working with exported structs. Also,
// NEVER pass in something that is not a pointer to a struct, as this
// will cause a panic.
func CopyStructToMap(sourceStruct interface{}, destinationMap map[string]bigquery.Value) {
	structToCopy := reflect.ValueOf(sourceStruct).Elem()
	typeOfStruct := structToCopy.Type()
	for i := 0; i < typeOfStruct.NumField(); i++ {
		destinationMap[strings.ToLower(typeOfStruct.Field(i).Name)] =
			structToCopy.Field(i).Interface()
	}

}

// GetMetaData combines the functionality of QueryAnnotationService and
// ParseJSONMetaDataResponse to query the annotator service and return
// the corresponding MetaData if it can, or a nil pointer if it
// encounters any error and cannot get the data for any reason
func GetMetaData(url string) *schema.MetaData {
	// Query the service and grab the response safely
	annotatorResponse, err := QueryAnnotationService(url)
	if err != nil {
		metrics.AnnotationErrorCount.
			With(prometheus.Labels{"source": "Error querying annotation service"}).Inc()
		log.Println(err)
		return nil
	}

	// Safely parse the JSON response and pass it back to the caller
	metaDataFromResponse, err := ParseJSONMetaDataResponse(annotatorResponse)
	if err != nil {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "Failed to parse JSON"}).Inc()
		log.Println(err)
		return nil
	}
	return metaDataFromResponse
}

// QueryAnnotationService will connect to the annotation service and
// copy the body of a valid response to a byte slice and return it to a
// user, returning an error if any occurs
func QueryAnnotationService(url string) ([]byte, error) {
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

// ParseJSONMetaDataResponse takes a byte slice containing the test of
// the JSON from the annotator service and parses it into a MetaData
// struct, for easy manipulation. It returns a pointer to the struct on
// success and an error if an error occurs.
func ParseJSONMetaDataResponse(jsonBuffer []byte) (*schema.MetaData, error) {
	parsedJSON := &schema.MetaData{}
	err := json.Unmarshal(jsonBuffer, parsedJSON)
	if err != nil {
		return nil, err
	}
	return parsedJSON, nil
}
