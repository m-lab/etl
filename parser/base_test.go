package parser_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/m-lab/annotation-service/api"
	v2as "github.com/m-lab/annotation-service/api/v2"
	"github.com/m-lab/etl/parser"
)

// Implement parser.Annotatable

type Row struct {
	client    string
	server    string
	clientAnn *api.Annotations
	serverAnn *api.Annotations
}

type BadRow struct{}

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

func assertAnnotatable(r *Row) {
	func(parser.Annotatable) {}(r)
}

func TestBase(t *testing.T) {
	os.Setenv("RELEASE_TAG", "foobar")
	parser.InitParserVersionForTest()

	ins := &inMemoryInserter{}

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

	b := parser.NewBase(ins, 10, v2as.GetAnnotator(ts.URL))

	err := b.AddRow(&Row{"1.2.3.4", "4.3.2.1", nil, nil})
	if err != nil {
		t.Error(err)
	}
	err = b.Annotate("tablename")
	if err != nil {
		t.Error(err)
	}
	if callCount != 2 {
		t.Error("Callcount should be 2:", callCount)
	}
	b.Flush()
	if ins.Committed() != 1 {
		t.Fatalf("Expected %d, Got %d.", 1, ins.Committed())
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

	err = b.AddRow(&BadRow{})
	if err != parser.ErrNotAnnotatable {
		t.Error("Should return ErrNotAnnotatable")
	}
}

func TestEmptyAnnotations(t *testing.T) {
	os.Setenv("RELEASE_TAG", "foobar")
	parser.InitParserVersionForTest()

	ins := &inMemoryInserter{}

	// Set up fake annotation service
	emptyResponse := `{"AnnotatorDate":"2018-12-05T00:00:00Z",
					  "Annotations":{}}`

	callCount := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, emptyResponse)
		callCount++
	}))
	defer func() {
		ts.Close()
	}()

	b := parser.NewBase(ins, 10, v2as.GetAnnotator(ts.URL))

	err := b.AddRow(&Row{"1.2.3.4", "4.3.2.1", nil, nil})
	if err != nil {
		t.Error(err)
	}
	err = b.Annotate("tablename")
	if err != nil {
		t.Error(err)
	}
	if callCount != 2 {
		t.Error("Callcount should be 2:", callCount)
	}
	b.Flush()
	if ins.Committed() != 1 {
		t.Fatalf("Expected %d, Got %d.", 1, ins.Committed())
	}

	if len(ins.data) != 1 {
		t.Fatal("Should have at one inserted row")
	}
	inserted := ins.data[0].(*Row)
	if inserted.clientAnn != nil {
		t.Error("clientAnn should be nil:", inserted.clientAnn)
	}
	if inserted.serverAnn != nil {
		t.Error("serverAnn should be nil:", inserted.serverAnn)
	}
}
