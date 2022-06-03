package parser_test

import (
	"testing"
	"time"

	"github.com/m-lab/annotation-service/api"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/parser"
)

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

func TestBase(t *testing.T) {
	ins := &inMemoryInserter{}

	b := parser.NewBase(ins, 10)

	err := b.AddRow(&Row{"1.2.3.4", "4.3.2.1", nil, nil})
	if err != nil {
		t.Error(err)
	}

	// Add a row with empty server IP
	err = b.AddRow(&Row{"1.2.3.4", "", nil, nil})
	if err != nil {
		t.Error(err)
	}

	b.Flush()
	if ins.Committed() != 2 {
		t.Fatalf("Expected %d, Got %d.", 2, ins.Committed())
	}

	if len(ins.data) < 1 {
		t.Fatal("Should have at least one inserted row")
	}
}

func TestAsyncPut(t *testing.T) {
	ins := &inMemoryInserter{}

	b := parser.NewBase(ins, 1)

	err := b.AddRow(&Row{"1.2.3.4", "4.3.2.1", nil, nil})
	if err != nil {
		t.Error(err)
	}

	if ins.Committed() != 0 {
		t.Fatalf("Expected %d, Got %d.", 0, ins.Committed())
	}

	err = b.AddRow(&Row{"1.2.3.4", "4.3.2.1", nil, nil})
	if err != etl.ErrBufferFull {
		t.Error("Should be full buffer error:", err)
	}

	b.PutAsync(b.TakeRows())
	b.Inserter.Flush() // To synchronize after the PutAsync.

	if ins.Committed() != 1 {
		t.Fatalf("Expected %d, Got %d.", 1, ins.Committed())
	}

	if len(ins.data) < 1 {
		t.Fatal("Should have at least one inserted row")
	}
}

func TestEmptyAnnotations(t *testing.T) {
	ins := &inMemoryInserter{}

	b := parser.NewBase(ins, 10)

	err := b.AddRow(&Row{"1.2.3.4", "4.3.2.1", nil, nil})
	if err != nil {
		t.Error(err)
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
