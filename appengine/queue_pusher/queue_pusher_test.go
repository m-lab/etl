package pushqueue

// TODO
// 1. Convert most of the tests to use an actual server.

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

func init() {
	// Always prepend the filename and line number.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func TestDefaultHandler(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "http://foobar.com/", nil)
	defaultHandler(w, r)
	if w.Result().StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(w.Body)
		log.Println(string(b))
		t.Error(w.Result().StatusCode)
	}

	// TODO - is this working as intended?
	r = httptest.NewRequest("POST", "http://foobar.com/", nil)
	defaultHandler(w, r)
	if w.Result().StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(w.Body)
		log.Println(string(b))
		t.Error(w.Result().StatusCode)
	}
}

func TestStats(t *testing.T) {
	tests := []struct {
		name   string
		queue  string
		status int
	}{
		{
			name:   "blank",
			queue:  "",
			status: http.StatusBadRequest,
		},
		{
			name:   "ndt",
			queue:  "etl-ndt-queue",
			status: http.StatusOK,
		},
		{
			// We should allow querying any arbitrary queue status...
			name:   "other",
			queue:  "other-queue",
			status: http.StatusOK,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			r := httptest.NewRequest(`GET`, `http://foobar.com/stats?queuename=`+tt.queue+`&test-bypass=true`, nil)
			queueStats(w, r)
			if w.Result().StatusCode != tt.status {
				b, _ := ioutil.ReadAll(w.Body)
				log.Println(string(b))
				t.Error(w.Result().StatusCode)
			}
		})
	}
}

func TestReceiver(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		queue    string
		status   int
	}{
		// TODO: Add test cases.
		{
			// This will fail GetFilename, which tries to base64 decode if it doesn't start with gs://
			name:     "xgs",
			filename: `xgs://m-lab-sandbox/ndt/2016/01/26/20160126T123456Z-mlab1-prg01-ndt-0007.tgz`,
			queue:    "",
			status:   http.StatusBadRequest,
		},
		{
			name:     ".baz",
			filename: `gs://m-lab-sandbox/ndt/2016/01/26/20160126T000000Z-mlab1-prg01-ndt-0007.gz.baz`,
			queue:    "",
			status:   http.StatusBadRequest,
		},
		{
			name:     "-pod1", // should have two digit pod index
			filename: `gs://m-lab-sandbox/ndt/2016/01/26/20160126T000000Z-mlab1-prg1-ndt-0007.tar.gz`,
			queue:    "",
			status:   http.StatusBadRequest,
		},
		{
			name:     "ok2",
			filename: `gs://m-lab-sandbox/ndt/2016/01/26/20160126T000000Z-mlab1-prg01-ndt-0007.tgz`,
			queue:    "",
			status:   http.StatusOK,
		},
		{
			name:     "ok3",
			filename: `gs://m-lab-sandbox/ndt/2016/07/14/20160714T123456Z-mlab1-lax04-ndt-0001.tar`,
			queue:    "",
			status:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reqStr string
			if tt.queue != "" {
				reqStr = "?filename=" + tt.filename + "&queue=" + tt.queue + "&test-bypass=true"
			} else {
				reqStr = "?filename=" + tt.filename + "&test-bypass=true"
			}
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "http://foobar.com/receiver"+reqStr, nil)
			receiver(w, r)
			b, _ := ioutil.ReadAll(w.Body)
			log.Println(string(b))
			if w.Result().StatusCode != tt.status {
				t.Error(w.Result().StatusCode)
			}
		})
	}
}

func TestReceiverWithQueue(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		queue    string
		status   int
	}{
		// TODO: Add test cases.
		{
			name:     "ok1",
			filename: `gs://m-lab-sandbox/ndt/2016/01/26/20160126T000000Z-mlab1-prg01-ndt-0007.tar.gz`,
			queue:    "etl-ndt-batch_0",
			status:   http.StatusOK,
		},
		{
			name:     "ok2",
			filename: `gs://m-lab-sandbox/ndt/2016/01/26/20160126T000000Z-mlab1-prg01-ndt-0007.tgz`,
			queue:    "etl-ndt-batch_1",
			status:   http.StatusOK,
		},
		{
			name:     "ok3",
			filename: `gs://m-lab-sandbox/ndt/2016/07/14/20160714T123456Z-mlab1-lax04-ndt-0001.tar`,
			queue:    "etl-ndt-batch_2",
			status:   http.StatusOK,
		},
		{
			// Should fail, because this is a daily pipeline queue.
			name:     "invalid",
			filename: `gs://m-lab-sandbox/ndt/2016/07/14/20160714T123456Z-mlab1-lax04-ndt-0001.tar`,
			queue:    "etl-ndt-queue",
			status:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var reqStr string
			if tt.queue != "" {
				reqStr = "?filename=" + tt.filename + "&queue=" + tt.queue + "&test-bypass=true"
			} else {
				reqStr = "?filename=" + tt.filename + "&test-bypass=true"
			}
			w := httptest.NewRecorder()
			r := httptest.NewRequest("GET", "http://foobar.com/receiver"+reqStr, nil)
			receiver(w, r)
			if w.Result().StatusCode != tt.status {
				b, _ := ioutil.ReadAll(w.Body)
				log.Println(string(b))
				t.Error(w.Result().StatusCode)
			}
		})
	}
}
