package parser

import (
	"bytes"
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
func FetchGeoAnnotations(ips []string, timestamp time.Time, geoDest []*schema.GeolocationIP) {
	reqData := make([]schema.RequestData, 0, len(ips))
	for _, ip := range ips {
		if ip == "" {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "Empty IP Address!!!"}).Inc()
			continue
		}
		reqData = append(reqData, schema.RequestData{ip, 0, timestamp})
	}
	annotationData := GetBatchMetaData(BatchURL, reqData)
	timeString := strconv.FormatInt(timestamp.Unix(), 36)
	for index, ip := range ips {
		data, ok := annotationData[ip+timeString]
		if !ok || data.Geo == nil {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "Missing or empty data for IP Address!!!"}).Inc()
			continue
		}
		*geoDest[index] = *data.Geo

	}
}

// AddMetaDataSSConnSpec takes a pointer to a
// Web100ConnectionSpecification struct and a timestamp. With these,
// it will fetch the appropriate metadata and add it to the hop struct
// referenced by the pointer.
func AddMetaDataSSConnSpec(spec *schema.Web100ConnectionSpecification, timestamp time.Time) {
	if spec == nil {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "SS ConnSpec was nil!!!"}).Inc()
		return
	}
	// Time the response
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.AnnotationTimeSummary.
			With(prometheus.Labels{"test_type": "SS"}).
			Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)

	ipSlice := []string{spec.Local_ip, spec.Remote_ip}
	geoSlice := []*schema.GeolocationIP{&spec.Local_geolocation, &spec.Remote_geolocation}
	FetchGeoAnnotations(ipSlice, timestamp, geoSlice)
}

// AddMetaDataPTConnSpec takes a pointer to a
// MLabConnectionSpecification struct and a timestamp. With these, it
// will fetch the appropriate metadata and add it to the hop struct
// referenced by the pointer.
func AddMetaDataPTConnSpec(spec *schema.MLabConnectionSpecification, timestamp time.Time) {
	if spec == nil {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "PT ConnSpec was nil!!!"}).Inc()
		return
	}
	// Time the response
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.AnnotationTimeSummary.
			With(prometheus.Labels{"test_type": "PT"}).
			Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)
	ipSlice := []string{spec.Server_ip, spec.Client_ip}
	geoSlice := []*schema.GeolocationIP{&spec.Server_geolocation, &spec.Client_geolocation}
	FetchGeoAnnotations(ipSlice, timestamp, geoSlice)
}

// AddMetaDataPTHopBatch takes a slice of pointers to
// schema.ParisTracerouteHops and will annotate all of them or fail
// silently. It sends them all in a single remote request.
func AddMetaDataPTHopBatch(hops []*schema.ParisTracerouteHop, timestamp time.Time) {
	// Time the response
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.AnnotationTimeSummary.
			With(prometheus.Labels{"test_type": "PT-HOP Batch"}).
			Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)
	requestSlice := CreateRequestDataFromPTHops(hops, timestamp)
	annotationData := GetBatchMetaData(BatchURL, requestSlice)
	AnnotatePTHops(hops, annotationData, timestamp)
}

// AnnotatePTHops takes a slice of hop pointers, the annotation data
// mapping ip addresses to metadata and a timestamp. It will then use
// these to attach the appropriate metadata to the PT hops.
func AnnotatePTHops(hops []*schema.ParisTracerouteHop, annotationData map[string]schema.MetaData, timestamp time.Time) {
	if annotationData == nil {
		return
	}
	timeString := strconv.FormatInt(timestamp.Unix(), 36)
	for _, hop := range hops {
		if hop == nil {
			continue
		}

		if data, ok := annotationData[hop.Src_ip+timeString]; ok && data.Geo != nil {
			hop.Src_geolocation = *data.Geo
		} else {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "Couldn't get geo data for PT Hop!"}).Inc()
		}

		if data, ok := annotationData[hop.Dest_ip+timeString]; ok && data.Geo != nil {
			hop.Dest_geolocation = *data.Geo
		} else {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "Couldn't get geo data for PT Hop!"}).Inc()
		}
	}
}

// CreateRequestDataFromPTHops will take a slice of PT hop pointers
// and the associate timestamp. From those, it will create a slice of
// requests to send to the annotation service, removing duplicates
// along the way.
func CreateRequestDataFromPTHops(hops []*schema.ParisTracerouteHop, timestamp time.Time) []schema.RequestData {
	hopMap := map[string]schema.RequestData{}
	for _, hop := range hops {
		if hop == nil {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "PT Hop was nil!!!"}).Inc()
			continue
		}
		if hop.Src_ip != "" {
			hopMap[hop.Src_ip] = schema.RequestData{hop.Src_ip, 0, timestamp}
		} else {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "PT Hop was missing an IP!!!"}).Inc()
		}

		if hop.Dest_ip != "" {
			hopMap[hop.Dest_ip] = schema.RequestData{hop.Dest_ip, 0, timestamp}
		} else {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "PT Hop was missing an IP!!!"}).Inc()
		}
	}

	requestSlice := make([]schema.RequestData, 0, len(hopMap))
	for _, req := range hopMap {
		requestSlice = append(requestSlice, req)
	}
	return requestSlice
}

// AddMetaDataPTHop takes a pointer to a ParisTracerouteHop and a
// timestamp. With these, it will fetch the appropriate metadata and
// add it to the hop struct referenced by the pointer.
func AddMetaDataPTHop(hop *schema.ParisTracerouteHop, timestamp time.Time) {
	if hop == nil {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "PT Hop was nil!!!"}).Inc()
		return
	}
	// Time the response
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.AnnotationTimeSummary.
			With(prometheus.Labels{"test_type": "PT-HOP"}).
			Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)
	if hop.Src_ip != "" {
		GetAndInsertGeolocationIPStruct(&hop.Src_geolocation, hop.Src_ip, timestamp)
	} else {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "PT Hop had no src_ip!"}).Inc()
	}
	if hop.Dest_ip != "" {
		GetAndInsertGeolocationIPStruct(&hop.Dest_geolocation, hop.Dest_ip, timestamp)
	} else {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "PT Hop had no dest_ip!"}).Inc()
	}
}

// GetAndInsertGeolocationIPStruct takes a NON-NIL pointer to a
// pre-allocated schema.GeolocationIP struct, an IP address, and a
// timestamp. It will connect to the annotation service, get the
// metadata, and insert the metadata into the reigion pointed to by
// the schema.GeolocationIP pointer.
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

	GetAndInsertTwoSidedMetaIntoNDTConnSpec(spec, timestamp)
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
		v := structToCopy.Field(i).Interface()
		switch t := v.(type) {
		case string:
			if t == "" {
				continue
			}
		case int64:
			if t == 0 {
				continue
			}
		}
		destinationMap[strings.ToLower(typeOfStruct.Field(i).Name)] = v

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

// GetAndInsertTwoSidedMetaIntoNDTConnSpec takes a timestamp and an
// NDT connection spec. It will either insert the data into the
// connection spec or silently fail.
func GetAndInsertTwoSidedMetaIntoNDTConnSpec(spec schema.Web100ValueMap, timestamp time.Time) {
	// TODO(JM): Make metrics for sok and cok failures. And double check metrics for cleanliness.
	cip, cok := spec.GetString([]string{"client_ip"})
	sip, sok := spec.GetString([]string{"server_ip"})
	reqData := []schema.RequestData{}
	if cok {
		reqData = append(reqData, schema.RequestData{IP: cip, Timestamp: timestamp})
	} else {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "Missing client side IP."}).Inc()
	}
	if sok {
		reqData = append(reqData, schema.RequestData{IP: sip, Timestamp: timestamp})
	} else {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "Missing server side IP."}).Inc()
	}
	if cok || sok {
		annotationDataMap := GetBatchMetaData(BatchURL, reqData)
		// TODO(JM): Revisit decision to use base36 for
		// encoding, rather than base64. (It had to do with
		// library support.)
		timeString := strconv.FormatInt(timestamp.Unix(), 36)
		if cok {
			if data, ok := annotationDataMap[cip+timeString]; ok && data.Geo != nil {
				CopyStructToMap(data.Geo, spec.Get("client_geolocation"))
			} else {
				metrics.AnnotationErrorCount.With(prometheus.
					Labels{"source": "Couldn't get metadata for the client side."}).Inc()
			}
		}
		if sok {
			if data, ok := annotationDataMap[sip+timeString]; ok && data.Geo != nil {
				CopyStructToMap(data.Geo, spec.Get("server_geolocation"))
			} else {
				metrics.AnnotationErrorCount.With(prometheus.
					Labels{"source": "Couldn't get metadata for the server side."}).Inc()
			}

		}
	}

}

// GetBatchMetaData combines the functionality of
// BatchQueryAnnotationService and BatchParseJSONMetaDataResponse to
// query the annotator service and return the corresponding map of
// ip-timestamp strings to schema.MetaData structs, or a nil map if it
// encounters any error and cannot get the data for any reason
func GetBatchMetaData(url string, data []schema.RequestData) map[string]schema.MetaData {
	// Query the service and grab the response safely
	annotatorResponse, err := BatchQueryAnnotationService(url, data)
	if err != nil {
		metrics.AnnotationErrorCount.
			With(prometheus.Labels{"source": "Error querying annotation service"}).Inc()
		log.Println(err)
		return nil
	}

	// Safely parse the JSON response and pass it back to the caller
	metaDataFromResponse, err := BatchParseJSONMetaDataResponse(annotatorResponse)
	if err != nil {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "Failed to parse JSON"}).Inc()
		log.Println(err)
		return nil
	}
	return metaDataFromResponse
}

// BatchQueryAnnotationService takes a url to POST the request to and
// a slice of schema.RequestDatas to be sent in the body in a JSON
// format. It will copy the response into a []byte and return it to
// the user, returning an error if any occurs
func BatchQueryAnnotationService(url string, data []schema.RequestData) ([]byte, error) {
	encodedData, err := json.Marshal(data)
	if err != nil {
		metrics.AnnotationErrorCount.
			With(prometheus.Labels{"source": "Couldn't Marshal Data"}).Inc()
		return nil, err
	}
	// Make the actual request
	resp, err := http.Post(url, "raw", bytes.NewReader(encodedData))

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

// BatchParseJSONMetaDataResponse takes a byte slice containing the
// text of the JSON from the annoator service's batch request endpoint
// and parses it into a map of strings to schema.MetaData structs, for
// easy manipulation. It returns a pointer to the struct on success
// and an error if one occurs.
func BatchParseJSONMetaDataResponse(jsonBuffer []byte) (map[string]schema.MetaData, error) {
	parsedJSON := make(map[string]schema.MetaData)
	err := json.Unmarshal(jsonBuffer, &parsedJSON)
	if err != nil {
		return nil, err
	}
	return parsedJSON, nil
}
