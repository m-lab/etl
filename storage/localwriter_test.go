package storage_test

import (
	"context"
	"os"
	"testing"

	"github.com/m-lab/go/testingx"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/row"
	"github.com/m-lab/go/rtx"

	"github.com/m-lab/etl/storage"
)

func TestLocalWriter_Commit(t *testing.T) {
	tests := []struct {
		name               string
		dir                string
		path               string
		count              int
		wantCommitJSONErr  bool
		wantCommitWriteErr bool
	}{
		{
			name:  "success",
			dir:   "testdir",
			path:  "this/is/a/test.json",
			count: 1,
		},
		{
			name:              "error-commit-json",
			dir:               "testdir",
			path:              "this/is/a/test.json",
			wantCommitJSONErr: true,
		},
		{
			name:               "error-commit-write",
			dir:                "testdir",
			path:               "this/is/a/test.json",
			wantCommitWriteErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := storage.NewLocalWriter(tt.dir, tt.path)
			if err != nil {
				t.Errorf("NewLocalWriter() error = %v, wantErr nil", err)
				return
			}
			defer func() {
				if tt.dir != "" {
					os.RemoveAll(tt.dir)
				}
			}()
			rows := []interface{}{
				"test",
			}
			if tt.wantCommitJSONErr {
				// Append function pointer which will generate a JSON marshal error.
				rows = append(rows, interface{}(got.Commit))
			}
			if tt.wantCommitWriteErr {
				// force close before trying to commit values.
				got.Close()
			}
			n, err := got.Commit(rows, "fake-output")
			if n != tt.count {
				t.Fatalf("LocalWriter.Commit() wrong count; got = %d, want %d", n, tt.count)
			}
			if (err != nil) != (tt.wantCommitJSONErr || tt.wantCommitWriteErr) {
				t.Fatalf("LocalWriter.Commit() wrong err; got = %v, wantCommitJSONErr %v", err, tt.wantCommitJSONErr)
			}
			err = got.Close()
			if (err != nil) != tt.wantCommitWriteErr {
				t.Fatalf("LocalWriter.Close() error = %v, want nil", err)
			}
		})
	}
}

func TestLocalWriter_Close(t *testing.T) {
	tests := []struct {
		name    string
		dir     string
		path    string
		count   int
		wantErr bool
	}{
		{
			name:  "success",
			dir:   "testdir",
			path:  "this/is/a/test.json",
			count: 1,
		},
		{
			name:    "error-commit-json",
			dir:     "testdir",
			path:    "this/is/a/test.json",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got row.Sink
			var err error
			if tt.wantErr {
				// Explicit allocation will create nil file pointer, so Close will fail.
				got = &storage.LocalWriter{}
			} else {
				got, err = storage.NewLocalWriter(tt.dir, tt.path)
				if err != nil {
					t.Errorf("NewLocalWriter() error = %v, wantErr nil", err)
					return
				}
				defer func() {
					if tt.dir != "" {
						os.RemoveAll(tt.dir)
					}
				}()
			}

			err = got.Close()
			if (err != nil) != tt.wantErr {
				t.Fatalf("LocalWriter.Close() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func TestNewLocalWriter(t *testing.T) {
	tests := []struct {
		name        string
		dir         string
		path        string
		wantOpenErr bool
	}{
		{
			name: "success",
			dir:  "testdir",
			path: "this/is/a/test.json",
		},
		{
			name:        "error-open",
			dir:         "testdir",
			path:        "this",
			wantOpenErr: true,
		},
		{
			name:        "error-mkdir",
			dir:         "testdir",
			path:        "this/file.dir",
			wantOpenErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantOpenErr {
				// Make directory so open will fail.
				err := os.MkdirAll(tt.dir, os.ModePerm)
				testingx.Must(t, err, "failed to mkdir")
				err = os.Chmod(tt.dir, 0000)
				testingx.Must(t, err, "failed to chmod")
			}
			got, err := storage.NewLocalWriter(tt.dir, tt.path)
			defer func() {
				if tt.dir != "" {
					os.RemoveAll(tt.dir)
				}
			}()

			if tt.wantOpenErr {
				if err == nil {
					t.Errorf("NewLocalWriter() wantOpenErr %v, want error", err)
				}
				return
			}

			err = got.Close()
			if err != nil {
				t.Errorf("LocalWriter.Close() error = %v, want nil", err)
			}
		})
	}
}
func TestNewLocalFactory(t *testing.T) {
	tests := []struct {
		name        string
		outputDir   string
		wantPathErr bool
		wantOpenErr bool
	}{
		{
			name:      "success",
			outputDir: t.TempDir(),
		},
		{
			name:        "error-path",
			outputDir:   t.TempDir(),
			wantPathErr: true,
		},
		{
			name:        "error-open",
			outputDir:   t.TempDir(),
			wantOpenErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lf := storage.NewLocalFactory(tt.outputDir)
			d, err := etl.ValidateTestPath("gs://bucket/exp/ndt7/2021/06/01/20210601T101003.000001Z-ndt7-mlab4-foo01-exp.tgz")
			rtx.Must(err, "failed to validate path")

			if tt.wantPathErr {
				d.URI = "gs://broken" // force URI parse to fail.
			}

			if tt.wantOpenErr {
				// Make directory so open will fail.
				err := os.MkdirAll(tt.outputDir, os.ModePerm)
				testingx.Must(t, err, "failed to mkdir")
				err = os.Chmod(tt.outputDir, 0000)
				testingx.Must(t, err, "failed to chmod")
			}

			lw, err := lf.Get(context.Background(), d)
			if (err != nil) != (tt.wantPathErr || tt.wantOpenErr) {
				t.Errorf("LocalFactory.Get() = %v, want %v", lw, tt.wantPathErr || tt.wantOpenErr)
			}
		})
	}
}
