// Copyright 2019 ETL Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//////////////////////////////////////////////////////////////////////////////

// generate_schema_docs uses ETL schema field descriptions to generate
// documentation in various formats.
package main

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/m-lab/go/rtx"
)

func Test_main(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "testing")
	rtx.Must(err, "Failed to create temporary directory")
	outputDirectory = tmpdir
	defer os.RemoveAll(tmpdir)

	main() // no crash == working

	// Check for expected files in tmpdir
	_, err = os.Stat(path.Join(tmpdir, "schema_ndtrow.md"))
	if err != nil {
		t.Errorf("main() missing output file; missing schema_ndtrow.md")
	}
}
