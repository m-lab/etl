package parser_test

import (
	"io/ioutil"
	"path"
	"strings"
	"testing"

	"cloud.google.com/go/civil"
	"github.com/go-test/deep"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
	"github.com/m-lab/go/pretty"
)

func setupNDT7InMemoryParser(t *testing.T, testName string) (*schema.NDT7ResultRow, error) {
	ins := newInMemorySink()
	n := parser.NewNDT7ResultParser(ins, "test", "_suffix")

	resultData, err := ioutil.ReadFile(path.Join("testdata/NDT7Result/", testName))
	if err != nil {
		t.Fatalf(err.Error())
	}
	meta := etl.ParserMetadata{
		ArchiveURL: "gs://mlab-test-bucket/ndt/ndt7/2020/03/18/ndt_ndt7_2020_03_18_20200318T003853.425987Z-ndt7-mlab3-syd03-ndt.tgz",
		Date:       civil.Date{Year: 2020, Month: 3, Day: 18},
		Version:    parser.Version(),
		GitCommit:  parser.GitCommit(),
	}
	err = n.ParseAndInsert(meta, testName, resultData)
	if err != nil {
		return nil, err
	}
	if n.Accepted() != 1 {
		t.Fatal("Failed to insert snaplog data.", ins)
	}
	n.Flush()
	row := ins.data[0].(*schema.NDT7ResultRow)
	return row, err
}

func TestNDT7ResultParser_ParseAndInsert(t *testing.T) {
	tests := []struct {
		name     string
		testName string
		wantErr  bool
	}{
		{
			name:     "success-download",
			testName: `ndt7-download-20200318T000657.568382877Z.ndt-knwp4_1583603744_000000000000590E.json`,
		},
		{
			name:     "success-upload",
			testName: `ndt7-upload-20200318T001352.496224022Z.ndt-knwp4_1583603744_0000000000005CF2.json`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row, err := setupNDT7InMemoryParser(t, tt.testName)
			if (err != nil) != tt.wantErr {
				t.Errorf("NDT7ResultParser.ParseAndInsert() error = %v, wantErr %v", err, tt.wantErr)
			}
			if row.Raw.Download != nil {
				exp := schema.NDT7Summary{
					UUID:               "ndt-knwp4_1583603744_000000000000590E",
					TestTime:           row.A.TestTime,
					CongestionControl:  "bbr",
					MeanThroughputMbps: 38.714033637501984,
					MinRTT:             285.804,
					LossRate:           0.12029169202467564,
				}
				if diff := deep.Equal(row.A, exp); diff != nil {
					t.Errorf("NDT7ResultParser.ParseAndInsert() different summary: %s", strings.Join(diff, "\n"))
				}

				expPI := schema.ParseInfo{
					Version:    "https://github.com/m-lab/etl/tree/foobar", // "local development",
					Time:       row.Parser.Time,                            // cheat a little, since this value should be about now.
					ArchiveURL: "gs://mlab-test-bucket/ndt/ndt7/2020/03/18/ndt_ndt7_2020_03_18_20200318T003853.425987Z-ndt7-mlab3-syd03-ndt.tgz",
					Filename:   "ndt7-download-20200318T000657.568382877Z.ndt-knwp4_1583603744_000000000000590E.json",
					Priority:   0,
					GitCommit:  "12345678",
				}
				if diff := deep.Equal(row.Parser, expPI); diff != nil {
					pretty.Print(row.Parser)
					t.Errorf("NDT7ResultParser.ParseAndInsert() different summary: %s", strings.Join(diff, "\n"))
				}

				dl := row.Raw.Download
				if len(dl.ServerMeasurements) != 33 {
					t.Errorf("NDT7ResultParser.ParseAndInsert() found wrong download measurements; got %d, want %d", len(dl.ServerMeasurements), 33)
				}
				if dl.ServerMeasurements[0].TCPInfo == nil {
					t.Errorf("NDT7ResultParser.ParseAndInsert() download measurements with nil TCPInfo")
				}
				if dl.ServerMeasurements[0].BBRInfo == nil {
					t.Errorf("NDT7ResultParser.ParseAndInsert() download measurements with nil BBRInfo")
				}
				if len(dl.ClientMetadata) != 6 {
					t.Errorf("NDT7ResultParser.ParseAndInsert() found wrong client metadata; got %d, want %d", len(dl.ClientMetadata), 6)
				}
			}
			if row.Raw.Upload != nil {
				exp := schema.NDT7Summary{
					UUID:               "ndt-knwp4_1583603744_0000000000005CF2",
					TestTime:           row.A.TestTime,
					CongestionControl:  "bbr",
					MeanThroughputMbps: 2.6848341983403983,
					MinRTT:             173.733,
					LossRate:           0,
				}
				if diff := deep.Equal(row.A, exp); diff != nil {
					t.Errorf("NDT7ResultParser.ParseAndInsert() different summary: %s", strings.Join(diff, "\n"))
				}
				expPI := schema.ParseInfo{
					Version:    "https://github.com/m-lab/etl/tree/foobar",
					Time:       row.Parser.Time,
					ArchiveURL: "gs://mlab-test-bucket/ndt/ndt7/2020/03/18/ndt_ndt7_2020_03_18_20200318T003853.425987Z-ndt7-mlab3-syd03-ndt.tgz",
					Filename:   "ndt7-upload-20200318T001352.496224022Z.ndt-knwp4_1583603744_0000000000005CF2.json",
					Priority:   0,
					GitCommit:  "12345678",
				}
				if diff := deep.Equal(row.Parser, expPI); diff != nil {
					t.Errorf("NDT7ResultParser.ParseAndInsert() different summary: %s", strings.Join(diff, "\n"))
				}
				up := row.Raw.Upload
				if len(up.ServerMeasurements) != 45 {
					t.Errorf("NDT7ResultParser.ParseAndInsert() found wrong upload measurements; got %d, want %d", len(up.ServerMeasurements), 45)
				}
				if up.ServerMeasurements[0].TCPInfo == nil {
					t.Errorf("NDT7ResultParser.ParseAndInsert() download measurements with nil TCPInfo")
				}
				if up.ServerMeasurements[0].BBRInfo == nil {
					t.Errorf("NDT7ResultParser.ParseAndInsert() download measurements with nil BBRInfo")
				}
				if len(up.ClientMetadata) != 6 {
					t.Errorf("NDT7ResultParser.ParseAndInsert() found wrong client metadata; got %d, want %d", len(up.ClientMetadata), 6)
				}
			}
			if row.Raw.Download == nil && row.Raw.Upload == nil {
				// We expect one or the other.
				t.Error("NDT7ResultParser.ParseAndInsert() found neither upload nor download result")
			}
		})
	}
}

func TestNDT7ResultParser_ParseAndInsertUnsafe(t *testing.T) {
	tests := []struct {
		name     string
		testName string
		wantErr  bool
	}{
		{
			name:     "success-remove-unsafe-uuid",
			testName: `ndt7-download-20221130T230746.606388371Z.ndt-rczlq_1666977535_unsafe_000000000016912D.json`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row, err := setupNDT7InMemoryParser(t, tt.testName)
			if (err != nil) != tt.wantErr {
				t.Errorf("NDT7ResultParser.ParseAndInsert() error = %v, wantErr %v", err, tt.wantErr)
			}
			if strings.Contains(row.ID, "_unsafe") || strings.Contains(row.A.UUID, "_unsafe") || strings.Contains(row.Raw.Download.UUID, "_unsafe") {
				t.Fatalf("ID or A.UUID contain the string '_unsafe'")
			}
		})
	}
}

func TestNDT7ResultParser_IsParsable(t *testing.T) {
	tests := []struct {
		name     string
		testName string
		want     bool
	}{
		{
			name:     "success-json",
			testName: `ndt7-download-20200318T000657.568382877Z.ndt-knwp4_1583603744_000000000000590E.json`,
			want:     true,
		},
		{
			name:     "error-bad-extension",
			testName: `badfile.badextension`,
			want:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := ioutil.ReadFile(`testdata/NDT7Result/` + tt.testName)
			if err != nil {
				t.Fatalf(err.Error())
			}
			p := &parser.NDT7ResultParser{}
			_, got := p.IsParsable(tt.testName, data)
			if got != tt.want {
				t.Errorf("NDT7ResultParser.IsParsable() got1 = %v, want %v", got, tt.want)
			}
		})
	}
}
