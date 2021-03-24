package annotation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"time"

	"github.com/m-lab/annotation-service/api"
	v2 "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/web100"

	"github.com/m-lab/etl/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// AnnotatorURL holds the https address of the annotator.
	AnnotatorURL string

	// BaseURL provides the base URL for single annotation requests
	BaseURL string

	// BatchURL provides the base URL for batch annotation requests
	BatchURL string
)

func SetupURLs(project string) {
	AnnotatorURL = "https://annotator-dot-" + project + ".appspot.com"
	BaseURL = AnnotatorURL + "/annotate?"
	BatchURL = AnnotatorURL + "/batch_annotate"
}

// trackMissingResponses generates metrics for missing annotations.
func trackMissingResponses(anno *api.GeoData) {
	if anno == nil {
		metrics.AnnotationMissingCount.WithLabelValues("nil-response").Inc()
		return
	}

	netOk := anno.Network != nil && len(anno.Network.Systems) > 0 && len(anno.Network.Systems[0].ASNs) > 0 && anno.Network.Systems[0].ASNs[0] != 0
	geoOk := anno.Geo != nil && anno.Geo.Latitude != 0 && anno.Geo.Longitude != 0

	if netOk && geoOk {
		return
	}
	if netOk {
		if anno.Geo == nil {
			metrics.AnnotationMissingCount.WithLabelValues("nil-geo").Inc()
		} else {
			metrics.AnnotationMissingCount.WithLabelValues("empty-geo").Inc()
		}
	} else if geoOk {
		if anno.Network == nil {
			metrics.AnnotationMissingCount.WithLabelValues("nil-asn").Inc()
		} else {
			metrics.AnnotationMissingCount.WithLabelValues("empty-asn").Inc()
		}
	} else {
		metrics.AnnotationMissingCount.WithLabelValues("both").Inc()
	}
}

func getAnnotations(ctx context.Context, timestamp time.Time, ips []string) ([]string, *v2.Response, error) {
	normalized := make([]string, len(ips))
	for i := range ips {
		if ips[i] == "" {
			// TODO(gfr) These should be warning, else we have error > request
			metrics.AnnotationWarningCount.With(prometheus.
				Labels{"source": "Empty IP Address!!!"}).Inc()
			continue
		}
		var err error
		normalized[i], err = web100.NormalizeIPv6(ips[i])
		if err != nil {
			log.Println(ips[i], err)
			metrics.AnnotationWarningCount.With(prometheus.
				Labels{"source": "NormalizeIPv6 Error"}).Inc()
		}
	}
	resp, err := v2.GetAnnotations(ctx, BatchURL, timestamp, normalized)
	if err != nil {
		log.Output(2, err.Error())
		// There are many error types returned here, so we log the error, but use the caller location
		// for the metric.
		_, file, line, _ := runtime.Caller(2)
		metrics.AnnotationErrorCount.With(prometheus.Labels{"source": fmt.Sprint(file, ":", line)}).Inc()
		metrics.AnnotationMissingCount.WithLabelValues("rpc error").Add(float64(len(ips)))
		return normalized, nil, err
	}

	if resp != nil {
		for _, anno := range resp.Annotations {
			trackMissingResponses(anno)
		}
	}

	return normalized, resp, err
}

// AddGeoAnnotations takes a slice of string ip addresses, a timestamp,
// and a slice of pointers to corresponding GeolocationIP structs.
// Slices must be the same length, or the function will immediately return.
// It calls the batch annotator, using the ip addresses and the timestamp and uses
// the response to fill in the structs pointed to by the slice of GeolocationIP pointers.
// Deprecated:  Use Annotatable interface and parser.Base instead.
func AddGeoAnnotations(ips []string, timestamp time.Time, geoDest []*api.GeolocationIP) {
	if ips == nil || geoDest == nil || len(ips) != len(geoDest) || len(ips) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	normalized, resp, err := getAnnotations(ctx, timestamp, ips)

	if err != nil {
		// getAnnotations should have logged error and updated metric.
		return
	}

	for i := range normalized {
		data, ok := resp.Annotations[normalized[i]]
		if !ok || data.Geo == nil {
			metrics.AnnotationWarningCount.With(prometheus.
				Labels{"source": "Missing or empty data for IP Address!!!"}).Inc()
			continue
		}
		*geoDest[i] = *data.Geo
	}
}

// FetchAllAnnotations takes a slice of strings containing ip addresses, a timestamp.
// It returns an array of pointers to GeoData structs corresponding to the ip addresses, or
// nil if there is an error.
// Deprecated:  Use Annotatable interface and parser.Base instead.
func FetchAllAnnotations(ips []string, timestamp time.Time) []*api.GeoData {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	normalized, resp, err := getAnnotations(ctx, timestamp, ips)

	if err != nil {
		return nil
	}

	geoDest := make([]*api.GeoData, len(ips)) // nil pointers

	for i := range normalized {
		data, ok := resp.Annotations[normalized[i]]
		if !ok || data.Geo == nil {
			metrics.AnnotationWarningCount.With(prometheus.
				Labels{"source": "Missing or empty data for IP Address!!!"}).Inc()
			continue
		}
		geoDest[i] = data
	}

	return geoDest
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
func ParseJSONGeoDataResponse(jsonBuffer []byte) (*api.GeoData, error) {
	parsedJSON := &api.GeoData{}
	err := json.Unmarshal(jsonBuffer, parsedJSON)
	if err != nil {
		return nil, err
	}
	return parsedJSON, nil
}
