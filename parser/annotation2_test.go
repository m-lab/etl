package parser_test

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/m-lab/etl/etl"

	"cloud.google.com/go/civil"
	"github.com/go-test/deep"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/rtx"
)

func TestAnnotation2Parser_ParseAndInsert(t *testing.T) {
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
			name: "success-empty-geo",
			file: "ndt-empty-geo.json",
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
			n := parser.NewAnnotation2Parser(ins, "test", "_suffix")

			data, err := ioutil.ReadFile("testdata/Annotation2/" + tt.file)
			rtx.Must(err, "failed to read test file")

			if _, ok := n.IsParsable(tt.file, data); !ok {
				t.Fatal("IsParsable() failed; got false, want true")
			}

			meta := etl.Metadata{
				ArchiveURL: "gs://mlab-test-bucket/ndt/ndt7/2020/03/18/" + tt.file,
				Date:       civil.Date{Year: 2020, Month: 3, Day: 18},
				Version:    parser.Version(),
				GitCommit:  parser.GitCommit(),
			}

			if err := n.ParseAndInsert(meta, tt.file, data); (err != nil) != tt.wantErr {
				t.Errorf("Annotation2Parser.ParseAndInsert() error = %v, wantErr %v", err, tt.wantErr)
			}

			if n.Accepted() == 1 {
				n.Flush()
				row := ins.data[0].(*schema.Annotation2Row)

				expPI := schema.ParseInfo{
					Version:    "https://github.com/m-lab/etl/tree/foobar",
					Time:       row.Parser.Time,
					ArchiveURL: "gs://mlab-test-bucket/ndt/ndt7/2020/03/18/" + tt.file,
					Filename:   tt.file,
					Priority:   0,
					GitCommit:  "12345678",
					FileSize:   int64(len(data)),
				}

				if diff := deep.Equal(row.Parser, expPI); diff != nil {
					t.Errorf("Annotation2Parser.ParseAndInsert() different summary: %s", strings.Join(diff, "\n"))
				}

				if row.Client.Geo != nil && row.Client.Geo.Region != "" {
					t.Errorf("Annotation2Parser.ParseAndInsert() did not clear Client.Geo.Region: %q", row.Client.Geo.Region)
				}
				if row.Server.Geo != nil && row.Server.Geo.Region != "" {
					t.Errorf("Annotation2Parser.ParseAndInsert() did not clear Server.Geo.Region: %q", row.Server.Geo.Region)
				}
			}
		})
	}
}
