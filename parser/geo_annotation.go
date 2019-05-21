package parser

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/m-lab/annotation-service/api"
	v2 "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/annotation"
	"github.com/m-lab/etl/web100"

	"cloud.google.com/go/bigquery"

	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
	"github.com/prometheus/client_golang/prometheus"
)

// AddGeoDataPTConnSpec takes a pointer to a
// MLabConnectionSpecification struct and a timestamp. With these, it
// will fetch the appropriate geo data and add it to the hop struct
// referenced by the pointer.
// Deprecated:  Should use batch annotation, with FetchAllAnnotations, as is done for SS
// in ss.Annotate prior to inserter.PutAsync.
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
// Deprecated:  Should use batch annotation, with FetchAllAnnotations, as is done for SS
// in ss.Annotate prior to inserter.PutAsync.
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
// Deprecated:  Should use batch annotation, with FetchAllAnnotations, as is done for SS
// in ss.Annotate prior to inserter.PutAsync.
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
// Deprecated:  Should use batch annotation, with FetchAllAnnotations, as is done for SS
// in ss.Annotate prior to inserter.PutAsync.
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

// AddGeoDataNDTConnSpec takes a connection spec and a timestamp and
// annotates the connection spec with geo data associated with each IP
// Address. It will either sucessfully add the geo data or fail
// silently and make no changes.
// Deprecated:  Should use batch annotation, with FetchAllAnnotations, as is done for SS
// in ss.Annotate prior to inserter.PutAsync.
func AddGeoDataNDTConnSpec(spec schema.Web100ValueMap, timestamp time.Time) {
	// Time the response
	timerStart := time.Now()
	defer func(tStart time.Time) {
		metrics.AnnotationTimeSummary.
			With(prometheus.Labels{"test_type": "NDT"}).
			Observe(float64(time.Since(tStart).Nanoseconds()))
	}(timerStart)

	GetAndInsertTwoSidedGeoIntoNDTConnSpec(spec, timestamp)
}

// CopyStructToMap takes a POINTER to an arbitrary SIMPLE struct and copies
// it's fields into a value map. It will also make fields entirely
// lower case, for convienece when working with exported structs. Also,
// NEVER pass in something that is not a pointer to a struct, as this
// will cause a panic.
func CopyStructToMap(sourceStruct interface{}, destinationMap map[string]bigquery.Value) {
	structToCopy := reflect.ValueOf(sourceStruct).Elem()
	typeOfStruct := structToCopy.Type()
	for i := 0; i < typeOfStruct.NumField(); i++ {
		f := structToCopy.Field(i)
		v := f.Interface()
		switch t := v.(type) {
		case string:
			// TODO - are these still needed?  Does the omitempty cover it?
			if t == "" {
				continue
			}
		case int64:
			if t == 0 {
				continue
			}
		}
		jsonTag, ok := typeOfStruct.Field(i).Tag.Lookup("json")
		name := strings.ToLower(typeOfStruct.Field(i).Name)
		if ok {
			tags := strings.Split(jsonTag, ",")
			if len(tags) > 0 && tags[0] != "" {
				name = tags[0]
			}
		}
		destinationMap[strings.ToLower(name)] = v
	}
}

// GetAndInsertTwoSidedGeoIntoNDTConnSpec takes a timestamp and an
// NDT connection spec. It will either insert the data into the
// connection spec or silently fail.
// TODO - should make a large batch request for an entire insert buffer.
// See sidestream implementation for example.
func GetAndInsertTwoSidedGeoIntoNDTConnSpec(spec schema.Web100ValueMap, timestamp time.Time) {
	// TODO: Make metrics for sok and cok failures. And double check metrics for cleanliness.
	cip, cok := spec.GetString([]string{"client_ip"})
	sip, sok := spec.GetString([]string{"server_ip"})
	reqData := make([]string, 0, 2)
	index := 0
	if cok {
		cip, _ = web100.NormalizeIPv6(cip)
		reqData = append(reqData, cip)
	} else {
		metrics.AnnotationWarningCount.With(prometheus.
			Labels{"source": "Missing client side IP."}).Inc()
	}
	if sok {
		sip, _ = web100.NormalizeIPv6(sip)
		reqData = append(reqData, sip)
	} else {
		metrics.AnnotationWarningCount.With(prometheus.
			Labels{"source": "Missing server side IP."}).Inc()
	}
	if cok || sok {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		deadline, _ := ctx.Deadline()
		defer cancel()
		resp, err := v2.GetAnnotations(ctx, annotation.BatchURL, timestamp, reqData)
		if err != nil {
			if err.Error() == "context canceled" {
				// These are NOT timeouts, and the ctx.Err() is nil.
				timeRemaining := deadline.Sub(time.Now())
				log.Println("context canceled, time remaining =", timeRemaining, " ctx err:", ctx.Err())
				_, file, line, _ := runtime.Caller(0)
				metrics.AnnotationErrorCount.With(prometheus.Labels{"source": fmt.Sprintf("context canceled %s:%d", file, line)}).Inc()
			} else {
				// There are many error types returned here, so we log the error, but use the code location
				// for the metric.
				log.Println(err)
				_, file, line, _ := runtime.Caller(0)
				metrics.AnnotationErrorCount.With(prometheus.Labels{"source": fmt.Sprint(file, ":", line)}).Inc()
			}
			return
		}

		if cok {
			if data, ok := resp.Annotations[cip]; ok && data.Geo != nil {
				CopyStructToMap(data.Geo, spec.Get("client_geolocation"))
				if data.Network != nil {
					asn, err := data.Network.BestASN()
					if err != nil {
						log.Println(err)
					} else {
						spec.Get("client").Get("network")["asn"] = asn
					}
				}
			} else {
				metrics.AnnotationErrorCount.With(prometheus.
					Labels{"source": "Couldn't get geo data for the client side."}).Inc()
			}
		}
		if sok {
			if data, ok := resp.Annotations[sip]; ok && data.Geo != nil {
				CopyStructToMap(data.Geo, spec.Get("server_geolocation"))
				if data.Network != nil {
					asn, err := data.Network.BestASN()
					if err != nil {
						log.Println(err)
					} else {
						spec.Get("server").Get("network")["asn"] = asn
					}
				}
			} else {
				metrics.AnnotationErrorCount.With(prometheus.
					Labels{"source": "Couldn't get geo data for the server side."}).Inc()
			}

		}
	}

}
