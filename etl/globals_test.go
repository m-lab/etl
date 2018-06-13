package etl_test

import (
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"runtime/debug"
	"testing"

	"github.com/m-lab/etl/etl"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestValidateTestPath(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		want    *etl.DataPath
		wantErr bool
	}{
		{
			name:    "error-bad-uri-prefix",
			path:    `xgs://m-lab-sandbox/ndt/2016/01/26/20160126T123456Z-mlab1-prg01-ndt-0007.tgz`,
			wantErr: true,
		},
		{
			name:    "error-bad-extension",
			path:    `gs://m-lab-sandbox/ndt/2016/01/26/20160126T000000Z-mlab1-prg01-ndt-0007.gz.baz`,
			wantErr: true,
		},
		{
			name:    "error-bad-pod-name",
			path:    `gs://m-lab-sandbox/ndt/2016/01/26/20160126T000000Z-mlab1-prg1-ndt-0007.tar.gz`,
			wantErr: true,
		},
		{
			name:    "error-bad-date-path",
			path:    `gs://m-lab-sandbox/ndt/2016/0126/20160126T000000Z-mlab1-prg1-ndt-0007.tar.gz`,
			wantErr: true,
		},
		{
			name: "success-tgz",
			path: `gs://m-lab-sandbox/ndt/2016/01/26/20160126T000000Z-mlab1-prg01-ndt-0007.tgz`,
			want: &etl.DataPath{
				"m-lab-sandbox", "ndt", "2016/01/26", "20160126", "000000", "mlab1", "prg01", "ndt", "0007", ".tgz",
			},
		},
		{
			name: "success-tar",
			path: `gs://m-lab-sandbox/ndt/2016/07/14/20160714T123456Z-mlab1-lax04-ndt-0001.tar`,
			want: &etl.DataPath{
				"m-lab-sandbox", "ndt", "2016/07/14", "20160714", "123456", "mlab1", "lax04", "ndt", "0001", ".tar",
			},
		},
		{
			name: "success-tar-gz",
			path: `gs://m-lab-sandbox/ndt/2016/07/14/20160714T123456Z-mlab1-lax04-ndt-0001.tar.gz`,
			want: &etl.DataPath{
				"m-lab-sandbox", "ndt", "2016/07/14", "20160714", "123456", "mlab1", "lax04", "ndt", "0001", ".tar.gz",
			},
		},
		{
			name: "success-embargo-tar-gz",
			path: `gs://embargo-mlab-oti/sidestream/2018/02/27/20180227T000010Z-mlab1-dfw02-sidestream-0000-e.tgz`,
			want: &etl.DataPath{
				"embargo-mlab-oti", "sidestream", "2018/02/27", "20180227", "000010", "mlab1", "dfw02", "sidestream", "0000", ".tgz",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := etl.ValidateTestPath(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateTestPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ValidateTestPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataPath_GetDataType(t *testing.T) {
	tests := []struct {
		name string
		exp1 string
		want etl.DataType
	}{
		{
			name: "okay",
			exp1: "ndt",
			want: etl.NDT,
		},
		{
			name: "invalid",
			exp1: "foobargum",
			want: etl.INVALID,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn := &etl.DataPath{
				Exp1: tt.exp1,
			}
			if got := fn.GetDataType(); got != tt.want {
				t.Errorf("DataPath.GetDataType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetMetroName(t *testing.T) {
	metro_name := etl.GetMetroName("20170501T000000Z-mlab1-acc02-paris-traceroute-0000.tgz")
	if metro_name != "acc" {
		fmt.Println(metro_name)
		t.Errorf("Error in getting metro name!\n")
		return
	}
}

func TestCalculateIPDistance(t *testing.T) {
	diff1, ip_type := etl.NumberBitsDifferent("192.168.3.4", "192.168.3.1")
	if diff1 != 3 || ip_type != 4 {
		t.Errorf("Error in calculating IPv4 distance!\n")
		return
	}
	diff2, ip_type := etl.NumberBitsDifferent("2001:0db8:85a3:0000:0000:8a2e:0370:7334", "2001:0db8:85a3:0000:0000:8a2e:0370:7334")
	if diff2 != 0 || ip_type != 6 {
		t.Errorf("Error in calculating IPv6 distance!\n")
		return
	}
}

func panicAndRecover() (err error) {
	defer func() {
		err = etl.PanicToErr(nil, recover(), "foobar")
	}()
	a := []int{1, 2, 3}
	log.Println(a[4])
	// This is never reached.
	return
}

func errorWithoutPanic(prior error) (err error) {
	err = prior
	defer func() {
		err = etl.PanicToErr(err, recover(), "foobar")
	}()
	return
}

func TestHandlePanic(t *testing.T) {
	err := panicAndRecover()
	log.Println("Actually did recover")
	if err == nil {
		t.Fatal("Should have errored")
	}
}

func TestNoPanic(t *testing.T) {
	err := errorWithoutPanic(nil)
	if err != nil {
		t.Error(err)
	}

	err = errorWithoutPanic(errors.New("prior"))
	if err.Error() != "prior" {
		t.Error("Should have returned prior error.")
	}
}

func rePanic() {
	defer func() {
		etl.CountPanics(recover(), "foobar")
	}()
	a := []int{1, 2, 3}
	log.Println(a[4])
	// This is never reached.
	return
}

func TestCountPanics(t *testing.T) {
	// When we call RePanic, the panic should cause a log and a metric
	// increment, but should still panic.  This intercepts the panic,
	// and errors if the panic doesn't happen.
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
		fmt.Printf("%s\n", debug.Stack())
	}()

	rePanic()
}

func TestDataset(t *testing.T) {
	tests := []struct {
		dt      etl.DataType
		isBatch bool
		want    string
	}{
		{etl.NDT, true, "batch"},
		{etl.NDT, false, "base_tables"},
		{etl.PT, true, "batch"},
		{etl.PT, false, "base_tables"},
		{etl.SS, true, "private"},
		{etl.SS, false, "private"},
	}

	// Project shouldn't matter, so test different values to confirm.
	os.Setenv("GCLOUD_PROJECT", "mlab-sandbox")
	for _, test := range tests {
		etl.IsBatch = test.isBatch
		got := test.dt.Dataset()
		if got != test.want {
			t.Errorf("for %s want: %s, got: %s.", test.dt, test.want, got)
		}
	}
	os.Setenv("GCLOUD_PROJECT", "mlab-oti")
	for _, test := range tests {
		etl.IsBatch = test.isBatch
		got := test.dt.Dataset()
		if got != test.want {
			t.Errorf("for %s want: %s, got: %s.", test.dt, test.want, got)
		}
	}

	override_tests := []struct {
		dt      etl.DataType
		isBatch bool
		want    string
	}{
		{etl.NDT, true, "override"},
		{etl.NDT, false, "override"},
		{etl.PT, true, "override"},
		{etl.PT, false, "override"},
		{etl.SS, true, "override"},
		{etl.SS, false, "override"},
	}

	// Test override
	os.Setenv("BIGQUERY_DATASET", "override")
	for _, test := range override_tests {
		etl.IsBatch = test.isBatch
		got := test.dt.Dataset()
		if got != test.want {
			t.Errorf("for %s want: %s, got: %s.", test.dt, test.want, got)
		}
	}

}

func TestBQProject(t *testing.T) {
	tests := []struct {
		dt       etl.DataType
		gproject string
		want     string
	}{
		{etl.NDT, "mlab-oti", "measurement-lab"},
		{etl.NDT, "staging", "staging"},
		{etl.PT, "mlab-oti", "measurement-lab"},
		{etl.SS, "mlab-oti", "mlab-oti"},
		{etl.SS, "mlab-oti", "mlab-oti"},
	}

	// isBatch  state shouldn't matter, so test with both values
	etl.IsBatch = true
	for _, test := range tests {
		os.Setenv("GCLOUD_PROJECT", test.gproject)
		got := test.dt.BigqueryProject()
		if got != test.want {
			t.Errorf("for %s,%s, want: %s, got: %s.", test.dt, test.gproject, test.want, got)
		}
	}
	etl.IsBatch = false
	for _, test := range tests {
		os.Setenv("GCLOUD_PROJECT", test.gproject)
		got := test.dt.BigqueryProject()
		if got != test.want {
			t.Errorf("for %s,%s, want: %s, got: %s.", test.dt, test.gproject, test.want, got)
		}
	}

	// Test override
	os.Setenv("BIGQUERY_PROJECT", "override_project")
	override_tests := []struct {
		dt       etl.DataType
		gproject string
		want     string
	}{
		{etl.NDT, "mlab-oti", "override_project"},
		{etl.NDT, "staging", "override_project"},
		{etl.PT, "mlab-oti", "override_project"},
		{etl.SS, "mlab-oti", "override_project"},
		{etl.SS, "mlab-oti", "override_project"},
	}
	etl.IsBatch = false
	for _, test := range override_tests {
		os.Setenv("GCLOUD_PROJECT", test.gproject)
		got := test.dt.BigqueryProject()
		if got != test.want {
			t.Errorf("for %s,%s, want: %s, got: %s.", test.dt, test.gproject, test.want, got)
		}
	}

}
