package parser

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/m-lab/etl/metrics"
	"github.com/m-lab/etl/schema"
	"github.com/prometheus/client_golang/prometheus"
)

//GetMetaData combines the functionality of QueryAnnotationService and
//ParseJSONMetaDataResponse to query the annotator service and return
//the corresponding MetaData if it can, or a nil pointer if it
//encounters any error and cannot get the data for any reason
func GetMetaData(url string) *schema.MetaData {
	//Query the service and grab the response safely
	annotatorResponse, err := QueryAnnotationService(url)
	if err != nil {
		metrics.AnnotationErrorCount.
			With(prometheus.Labels{"source": "Error querying annotation service"}).Inc()
		log.Println(err)
		return nil
	}

	//Safely parse the JSON response and pass it back to the caller
	metaDataFromResponse, err := ParseJSONMetaDataResponse(annotatorResponse)
	if err != nil {
		log.Println(err)
		return nil
	}
	return metaDataFromResponse
}

//QueryAnnotationService will connect to the annotation service and
//copy the body of a valid response to a byte slice and return it to a
//user, returning an error if any occurs
func QueryAnnotationService(url string) ([]byte, error) {
	//Make the actual request
	resp, err := http.Get(url)

	//Catch http errors
	if err != nil {
		metrics.AnnotationErrorCount.
			With(prometheus.Labels{"source": "Request to Annotator failed"}).Inc()
		return nil, err
	}

	//Catch errors reported by the service
	if resp.StatusCode != http.StatusOK {
		metrics.AnnotationErrorCount.
			With(prometheus.Labels{"source": "Webserver gave non-ok response"}).Inc()
		return nil, errors.New("URL:" + url + " gave response code " + resp.Status)
	}
	defer resp.Body.Close()

	//Copy response into a byte slice
	return ioutil.ReadAll(resp.Body)
}

//ParseJSONMetaDataResponse takes a byte slice containing the test of
//the JSON from the annotator service and parses it into a MetaData
//struct, for easy manipulation. It returns a pointer to the struct on
//success and an error if an error occurs.
func ParseJSONMetaDataResponse(jsonBuffer []byte) (*schema.MetaData, error) {
	parsedJSON := &schema.MetaData{}
	err := json.Unmarshal(jsonBuffer, parsedJSON)
	if err != nil {
		metrics.AnnotationErrorCount.With(prometheus.
			Labels{"source": "Failed to parse JSON"}).Inc()
		return nil, err
	}

	return parsedJSON, nil
}
