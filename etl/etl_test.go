package etl_test

import (
	"fmt"
	"testing"

	"github.com/m-lab/etl/etl"
)

func TestValidation(t *testing.T) {
	// These should fail:
	// Leading character before gs://
	_, err := etl.ValidateTestPath(
		`xgs://m-lab-sandbox/ndt/2016/01/26/20160126T000000Z-mlab1-prg01-ndt-0007.tgz`)
	if err == nil {
		t.Error("Should be invalid: ")
	}
	// Wrong trailing characters
	_, err = etl.ValidateTestPath(
		`gs://m-lab-sandbox/ndt/2016/01/26/20160126T000000Z-mlab1-prg01-ndt-0007.gz.baz`)
	if err == nil {
		t.Error("Should be invalid: ")
	}
	// Missing mlabN-podNN
	_, err = etl.ValidateTestPath(
		`gs://m-lab-sandbox/ndt/2016/01/26/20160126T000000Z-mlab1-prg1-ndt-0007.tar.gz`)
	if err == nil {
		t.Error("Should be invalid: ")
	}

	// These should succeed
	data, err := etl.ValidateTestPath(
		`gs://m-lab-sandbox/ndt/2016/01/26/20160126T000000Z-mlab1-prg01-ndt-0007.tgz`)
	if err != nil {
		t.Error(err)
	}
	data, err = etl.ValidateTestPath(
		`gs://m-lab-sandbox/ndt/2016/07/14/20160714T000000Z-mlab1-lax04-ndt-0001.tar`)
	if err != nil {
		t.Error(err)
	}
	data, err = etl.ValidateTestPath(
		`gs://m-lab-sandbox/ndt/2016/07/14/20160714T000000Z-mlab1-lax04-ndt-0001.tar.gz`)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("%v\n", data)
}
