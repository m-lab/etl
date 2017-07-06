package parser

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/metrics"
)

func addMetaData(geo *bigquery.Value, asn *bigquery.Value, ip string) {
	timerStart := time.Now()
	defer metrics.AnnotationTimeSummary.Observe(float64(time.Since(timerStart).Nanoseconds()))

	resp, err := http.Get("https://annotator-dot-mlab-sandbox.appspot.com/annotate?ip_addr=" + url.QueryEscape(ip) + "&since_epoch=" + strconv.FormatInt(time.Now().Unix(), 10))

	if err != nil {
		metrics.AnnotationErrorCount.Inc()
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		metrics.AnnotationErrorCount.Inc()
		return
	}
	var parsedJSON interface{}
	err = json.Unmarshal(body, &parsedJSON)
	if err != nil {
		metrics.AnnotationErrorCount.Inc()
		return
	}
	_, ok := parsedJSON.(map[string]bigquery.Value)
	if !ok {
		metrics.AnnotationErrorCount.Inc()
		return
	}
	// TODO(JM) verify that we are getting the right data in here
	// TODO(JM) punch the maps we get from the parsed JSON back into the maps passed into the function

}
