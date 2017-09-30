package parser

import (
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
	"github.com/prometheus/client_golang/prometheus"
)

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
<<<<<<< HEAD:parser/metadata.go
	geoSlice := []*annotation.GeolocationIP{&spec.Local_geolocation, &spec.Remote_geolocation}
	FetchGeoAnnotations(ipSlice, timestamp, geoSlice)
=======
	geoSlice := []*geo.GeolocationIP{&spec.Local_geolocation, &spec.Remote_geolocation}
	geo.FetchGeoAnnotations(ipSlice, timestamp, geoSlice)
>>>>>>> aa41f64... Finish fixing and rename metadata -> annotation:parser/annotation.go
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
<<<<<<< HEAD:parser/metadata.go
	geoSlice := []*annotation.GeolocationIP{&spec.Server_geolocation, &spec.Client_geolocation}
	FetchGeoAnnotations(ipSlice, timestamp, geoSlice)
=======
	geoSlice := []*geo.GeolocationIP{&spec.Server_geolocation, &spec.Client_geolocation}
	geo.FetchGeoAnnotations(ipSlice, timestamp, geoSlice)
>>>>>>> aa41f64... Finish fixing and rename metadata -> annotation:parser/annotation.go
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
	annotationData := geo.GetBatchMetaData(geo.BatchURL, requestSlice)
	AnnotatePTHops(hops, annotationData, timestamp)
}

// AnnotatePTHops takes a slice of hop pointers, the annotation data
// mapping ip addresses to metadata and a timestamp. It will then use
// these to attach the appropriate metadata to the PT hops.
func AnnotatePTHops(hops []*schema.ParisTracerouteHop, annotationData map[string]annotation.MetaData, timestamp time.Time) {
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
func CreateRequestDataFromPTHops(hops []*schema.ParisTracerouteHop, timestamp time.Time) []annotation.RequestData {
	hopMap := map[string]annotation.RequestData{}
	for _, hop := range hops {
		if hop == nil {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "PT Hop was nil!!!"}).Inc()
			continue
		}
		if hop.Src_ip != "" {
			hopMap[hop.Src_ip] = annotation.RequestData{hop.Src_ip, 0, timestamp}
		} else {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "PT Hop was missing an IP!!!"}).Inc()
		}

		if hop.Dest_ip != "" {
			hopMap[hop.Dest_ip] = annotation.RequestData{hop.Dest_ip, 0, timestamp}
		} else {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "PT Hop was missing an IP!!!"}).Inc()
		}
	}

	requestSlice := make([]annotation.RequestData, 0, len(hopMap))
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
		geo.GetAndInsertGeolocationIPStruct(&hop.Src_geolocation, hop.Src_ip, timestamp)
	} else {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "PT Hop had no src_ip!"}).Inc()
	}
	if hop.Dest_ip != "" {
		geo.GetAndInsertGeolocationIPStruct(&hop.Dest_geolocation, hop.Dest_ip, timestamp)
	} else {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "PT Hop had no dest_ip!"}).Inc()
	}
}

// AddMetaDataNDTConnSpec takes a connection spec and a timestamp and
// annotates the connection spec with metadata associated with each IP
// Address. It will either sucessfully add the metadata or fail
// silently and make no changes.
func AddMetaDataNDTConnSpec(spec schema.Web100ValueMap, timestamp time.Time) {
	// Only annotate if flag enabled...
	// TODO(gfr) - should propogate this to other pipelines, or push to a common
	// intercept point.
	if !geo.IPAnnotationEnabled {
		metrics.AnnotationErrorCount.With(prometheus.Labels{
			"source": "IP Annotation Disabled."}).Inc()
		return
	}

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
		url := geo.BaseURL + "ip_addr=" + url.QueryEscape(ip) +
			"&since_epoch=" + strconv.FormatInt(timestamp.Unix(), 10)
		annotationData := geo.GetMetaData(url)
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

// GetAndInsertTwoSidedMetaIntoNDTConnSpec takes a timestamp and an
// NDT connection spec. It will either insert the data into the
// connection spec or silently fail.
func GetAndInsertTwoSidedMetaIntoNDTConnSpec(spec schema.Web100ValueMap, timestamp time.Time) {
	// TODO(JM): Make metrics for sok and cok failures. And double check metrics for cleanliness.
	cip, cok := spec.GetString([]string{"client_ip"})
	sip, sok := spec.GetString([]string{"server_ip"})
	reqData := []annotation.RequestData{}
	if cok {
		reqData = append(reqData, annotation.RequestData{IP: cip, Timestamp: timestamp})
	} else {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "Missing client side IP."}).Inc()
	}
	if sok {
		reqData = append(reqData, annotation.RequestData{IP: sip, Timestamp: timestamp})
	} else {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "Missing server side IP."}).Inc()
	}
	if cok || sok {
		annotationDataMap := geo.GetBatchMetaData(geo.BatchURL, reqData)
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
