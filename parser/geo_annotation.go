package parser

import (
	"strconv"
	"time"

	"github.com/m-lab/annotation-service/api"
	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/web100"

	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
	"github.com/prometheus/client_golang/prometheus"
)

// AddGeoDataPTConnSpec takes a pointer to a
// MLabConnectionSpecification struct and a timestamp. With these, it
// will fetch the appropriate geo data and add it to the hop struct
// referenced by the pointer.
// Deprecated:  Use Annotatable interface and parser.Base instead.
func AddGeoDataPTConnSpec(spec *schema.MLabConnectionSpecification, timestamp time.Time) {
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
	geoSlice := []*api.GeolocationIP{&spec.Server_geolocation, &spec.Client_geolocation}
	annotation.AddGeoAnnotations(ipSlice, timestamp, geoSlice)
}

// AddGeoDataPTHopBatch takes a slice of pointers to
// schema.ParisTracerouteHops and will annotate all of them or fail
// silently. It sends them all in a single remote request.
// Deprecated:  Use Annotatable interface and parser.Base instead.
func AddGeoDataPTHopBatch(hops []*schema.ParisTracerouteHop, timestamp time.Time) {
	// Time the response
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.AnnotationTimeSummary.
			With(prometheus.Labels{"test_type": "PT-HOP Batch"}).
			Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)
	requestSlice := CreateRequestDataFromPTHops(hops, timestamp)
	annotationData := annotation.GetBatchGeoData(annotation.BatchURL, requestSlice)
	AnnotatePTHops(hops, annotationData, timestamp)
}

// AnnotatePTHops takes a slice of hop pointers, the annotation data
// mapping ip addresses to geo data and a timestamp. It will then use
// these to attach the appropriate geo data to the PT hops.
// Deprecated:  Use Annotatable interface and parser.Base instead.
func AnnotatePTHops(hops []*schema.ParisTracerouteHop, annotationData map[string]api.GeoData, timestamp time.Time) {
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
func CreateRequestDataFromPTHops(hops []*schema.ParisTracerouteHop, timestamp time.Time) []api.RequestData {
	hopMap := map[string]api.RequestData{}
	for _, hop := range hops {
		if hop == nil {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "PT Hop was nil!!!"}).Inc()
			continue
		}
		if hop.Src_ip != "" {
			hop.Src_ip, _ = web100.NormalizeIPv6(hop.Src_ip)
			hopMap[hop.Src_ip] = api.RequestData{
				IP: hop.Src_ip, IPFormat: 0, Timestamp: timestamp}
		} else {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "PT Hop was missing an IP!!!"}).Inc()
		}

		if hop.Dest_ip != "" {
			hop.Dest_ip, _ = web100.NormalizeIPv6(hop.Dest_ip)
			hopMap[hop.Dest_ip] = api.RequestData{
				IP: hop.Dest_ip, IPFormat: 0, Timestamp: timestamp}
		} else {
			metrics.AnnotationErrorCount.With(prometheus.
				Labels{"source": "PT Hop was missing an IP!!!"}).Inc()
		}
	}

	requestSlice := make([]api.RequestData, 0, len(hopMap))
	for _, req := range hopMap {
		requestSlice = append(requestSlice, req)
	}
	return requestSlice
}

// AddGeoDataPTHop takes a pointer to a ParisTracerouteHop and a
// timestamp. With these, it will fetch the appropriate geo data and
// add it to the hop struct referenced by the pointer.
// Deprecated:  Use Annotatable interface and parser.Base instead.
func AddGeoDataPTHop(hop *schema.ParisTracerouteHop, timestamp time.Time) {
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
		annotation.GetAndInsertGeolocationIPStruct(&hop.Src_geolocation, hop.Src_ip, timestamp)
	} else {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "PT Hop had no src_ip!"}).Inc()
	}
	if hop.Dest_ip != "" {
		annotation.GetAndInsertGeolocationIPStruct(&hop.Dest_geolocation, hop.Dest_ip, timestamp)
	} else {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "PT Hop had no dest_ip!"}).Inc()
	}
}
