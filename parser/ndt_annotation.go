package parser

import (
	"reflect"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/geo"
	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
	"github.com/prometheus/client_golang/prometheus"
)

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
	reqData := []geo.RequestData{}
	if cok {
		reqData = append(reqData, geo.RequestData{IP: cip, Timestamp: timestamp})
	} else {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "Missing client side IP."}).Inc()
	}
	if sok {
		reqData = append(reqData, geo.RequestData{IP: sip, Timestamp: timestamp})
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
