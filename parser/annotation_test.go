package parser_test

import (
	"io/ioutil"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/go/rtx"
)

func TestAnnotationParser_ParseAndInsert(t *testing.T) {
	tests := []struct {
		name    string
		file    string
		wantErr bool
	}{
		{
			name: "success",
			file: "ndt-njp6l_1585004303_00000000000170FA.json",
		},
		{
			name:    "corrupt-input",
			file:    "ndt-corrupt.json",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ins := newInMemorySink()
			n := parser.NewAnnotationParser(ins, "test", "_suffix", &fakeAnnotator{})

			data, err := ioutil.ReadFile("testdata/Annotation/" + tt.file)
			rtx.Must(err, "failed to read test file")

			if _, ok := n.IsParsable(tt.file, data); !ok {
				t.Fatal("IsParsable() failed; got false, want true")
			}

			meta := map[string]bigquery.Value{
				"filename": "gs://mlab-test-bucket/ndt/ndt7/2020/03/18/" + tt.file,
			}

			if err := n.ParseAndInsert(meta, tt.file, data); (err != nil) != tt.wantErr {
				t.Errorf("AnnotationParser.ParseAndInsert() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
