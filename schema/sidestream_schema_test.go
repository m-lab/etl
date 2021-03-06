package schema

import (
	"testing"
)

func TestSS_Schema(t *testing.T) {
	t.Run("schema-generate", func(t *testing.T) {
		ss := &SS{}
		sch, _ := ss.Schema()
		if len(sch) < 3 {
			t.Fatalf("SS.Schema() wrong length = got %d, want >= 3", len(sch))
		}

		if sch[3].Name != "log_time" && sch[3].Type != "TIMESTAMP" {
			t.Errorf("SS.Schema() wrong log_time timestamp = got %q, want %q", sch[3].Type, "TIMESTAMP")
		}
	})
}
