package row_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/row"

	"github.com/m-lab/annotation-service/api"
	v2as "github.com/m-lab/annotation-service/api/v2"
)

//lint:ignore U1000 compile time assertions
func assertTestRowAnnotatable(r *Row) {
	func(row.Annotatable) {}(r)
}

//lint:ignore U1000 compile time assertions
func assertSink(in row.Sink) {
	func(in row.Sink) {}(&inMemorySink{})
}

//lint:ignore U1000 compile time assertions
func assertBQInserterIsSink(in row.Sink) {
	func(in row.Sink) {}(&bq.BQInserter{})
}

// Implement parser.Annotatable

type Row struct {
	client    string
	server    string
	clientAnn *api.Annotations
	serverAnn *api.Annotations
}

func (row *Row) GetClientIPs() []string {
	return []string{row.client}
}

func (row *Row) GetServerIP() string {
	return row.server
}

func (row *Row) AnnotateClients(remote map[string]*api.Annotations) error {
	row.clientAnn = remote[row.GetClientIPs()[0]]
	return nil
}

func (row *Row) AnnotateServer(local *api.GeoData) error {
	row.serverAnn = local
	return nil
}

func (row *Row) GetLogTime() time.Time {
	return time.Now()
}

type inMemorySink struct {
	data      []interface{}
	committed int
	failed    int
}

func newInMemorySink() *inMemorySink {
	data := make([]interface{}, 0)
	return &inMemorySink{data, 0, 0}
}

func (in *inMemorySink) Commit(data []interface{}, label string) (int, error) {
	in.data = append(in.data, data...)
	in.committed = len(in.data)
	return len(data), nil
}

func (in *inMemorySink) Close() error { return nil }

func TestBase(t *testing.T) {
	ins := newInMemorySink()

	// Set up fake annotation service
	r1 := `{"AnnotatorDate":"2018-12-05T00:00:00Z",
	                  "Annotations":{"1.2.3.4":{"Geo":{"postal_code":"10583"}}}}`
	r2 := `{"AnnotatorDate":"2018-12-05T00:00:00Z",
					  "Annotations":{"4.3.2.1":{"Geo":{"postal_code":"10584"}}}}`

	callCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// HACKY - depends on order in which client and server are annotated
		if callCount == 0 {
			fmt.Fprint(w, r1)
		} else {
			fmt.Fprint(w, r2)
		}
		callCount++
	}))
	defer func() {
		ts.Close()
	}()

	b := row.NewBase("test", ins, 10, v2as.GetAnnotator(ts.URL))

	b.Put(&Row{"1.2.3.4", "4.3.2.1", nil, nil})

	// Add a row with empty server IP
	b.Put(&Row{"1.2.3.4", "", nil, nil})
	if callCount != 0 {
		t.Error("Callcount should be 0:", callCount)
	}

	b.Flush()
	if callCount != 2 {
		t.Error("Callcount should be 2:", callCount)
	}
	stats := b.GetStats()
	if stats.Committed != 2 {
		t.Fatalf("Expected %d, Got %d.", 2, stats.Committed)
	}

	if len(ins.data) < 1 {
		t.Fatal("Should have at least one inserted row")
	}
	inserted := ins.data[0].(*Row)
	if inserted.clientAnn == nil || inserted.clientAnn.Geo.PostalCode != "10583" {
		t.Error("Failed client annotation")
	}
	if inserted.serverAnn == nil || inserted.serverAnn.Geo.PostalCode != "10584" {
		t.Error("Failed server annotation")
	}
}

func TestAsyncPut(t *testing.T) {
	ins := newInMemorySink()

	// Set up fake annotation service
	r1 := `{"AnnotatorDate":"2018-12-05T00:00:00Z",
	                  "Annotations":{"1.2.3.4":{"Geo":{"postal_code":"10583"}}}}`
	r2 := `{"AnnotatorDate":"2018-12-05T00:00:00Z",
					  "Annotations":{"4.3.2.1":{"Geo":{"postal_code":"10584"}}}}`

	callCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// HACKY - depends on order in which client and server are annotated
		if callCount == 0 {
			fmt.Fprint(w, r1)
		} else {
			fmt.Fprint(w, r2)
		}
		callCount++
	}))
	defer func() {
		ts.Close()
	}()

	b := row.NewBase("test", ins, 1, v2as.GetAnnotator(ts.URL))

	b.Put(&Row{"1.2.3.4", "4.3.2.1", nil, nil})

	if b.GetStats().Committed != 0 {
		t.Fatalf("Expected %d, Got %d.", 0, b.GetStats().Committed)
	}

	// This should trigger an async flush
	b.Put(&Row{"1.2.3.4", "4.3.2.1", nil, nil})
	start := time.Now()
	for time.Since(start) < 5*time.Second && b.GetStats().Committed < 1 {
		time.Sleep(10 * time.Millisecond)
	}

	if b.GetStats().Committed != 1 {
		t.Fatalf("Expected %d, Got %d.", 1, b.GetStats().Committed)
	}

	if len(ins.data) < 1 {
		t.Fatal("Should have at least one inserted row")
	}
	inserted := ins.data[0].(*Row)
	if inserted.clientAnn == nil || inserted.clientAnn.Geo.PostalCode != "10583" {
		t.Error("Failed client annotation")
	}
	if inserted.serverAnn == nil || inserted.serverAnn.Geo.PostalCode != "10584" {
		t.Error("Failed server annotation")
	}
}
