package schema

import (
	"testing"
)

func TestSS_Schema(t *testing.T) {
	t.Run("schema-generate", func(t *testing.T) {
		ss := &SS{}
		sch, _ := ss.Schema()
		if sch[2].Type != "TIMESTAMP" {
			t.Errorf("SS.Schema() wrong log_time timestamp = got %q, want %q", sch[2].Type, "TIMESTAMP")
		}
	})
}
