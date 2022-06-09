package row_test

import (
	"errors"
	"testing"
	"time"

	"github.com/m-lab/etl/row"
)

// Implement parser.Annotatable

type Row struct {
	client string
	server string
}

type BadRow struct{}

func (row *Row) GetClientIPs() []string {
	return []string{row.client}
}

func (row *Row) GetServerIP() string {
	return row.server
}

func (row *Row) GetLogTime() time.Time {
	return time.Now()
}

func assertSink(in row.Sink) {
	func(in row.Sink) {}(&inMemorySink{})
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
	ins := &inMemorySink{}

	b := row.NewBase("test", ins, 10)

	b.Put(&Row{"1.2.3.4", "4.3.2.1"})

	// Add a row with empty server IP
	b.Put(&Row{"1.2.3.4", ""})
	b.Flush()
	stats := b.GetStats()
	if stats.Committed != 2 {
		t.Fatalf("Expected %d, Got %d.", 2, stats.Committed)
	}

	if len(ins.data) < 1 {
		t.Fatal("Should have at least one inserted row")
	}
}

func TestAsyncPut(t *testing.T) {
	ins := &inMemorySink{}

	b := row.NewBase("test", ins, 1)

	b.Put(&Row{"1.2.3.4", "4.3.2.1"})

	if b.GetStats().Committed != 0 {
		t.Fatalf("Expected %d, Got %d.", 0, b.GetStats().Committed)
	}

	// This should trigger an async flush
	b.Put(&Row{"1.2.3.4", "4.3.2.1"})
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
}

func TestErrCommitRow(t *testing.T) {
	baseErr := errors.New("googleapi.Error")
	commitErr := row.ErrCommitRow{baseErr}
	expectedMessage := "failed to commit row(s), error: googleapi.Error"

	if commitErr.Error() != expectedMessage {
		t.Errorf("ErrCommitRow.Error() failed error message, expected: %s, got: %s", expectedMessage, commitErr.Error())
	}

	if !errors.Is(commitErr.Unwrap(), baseErr) {
		t.Errorf("ErrCommitRow.Unwrap() failed to unwrap error, expected: %v, got: %v", baseErr, commitErr.Unwrap())
	}

	target := row.ErrCommitRow{}
	if !errors.As(commitErr, &target) {
		t.Errorf("ErrCommitRow.As() failed to recognize error as ErrCommitRow, expected: true, got: false")
	}
}
