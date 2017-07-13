// Parse Sidestream filename like 20170516T22:00:00Z_163.7.129.73_0.web100
package parser

import (
	"bufio"
	"cloud.google.com/go/bigquery"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/m-lab/etl/etl"
	"github.com/m-lab/etl/schema"
)

type SSParser struct {
	inserter etl.Inserter
}

func NewSSParser(ins etl.Inserter) *SSParser {
	return &SSParser{ins}
}

func ParseSSFilename(testName string) {
	return
}

func ParseIPFamily(ipStr string) int {
	ip := net.ParseIP(ipStr)
	if ip.To4() != nil {
		return syscall.AF_INET
	} else if ip.To16() != nil {
		return syscall.AF_INET6
	}
	return -1
}

func LoadLegacyMapping(fileName string) map[string]string {
	legacy_mapping := make(map[string]string)
	file, err := os.Open(fileName)
	if err != nil {
		return legacy_mapping
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		oneLine := strings.TrimSuffix(scanner.Text(), "\n")
		names := strings.Split(oneLine, " ")
		if len(names) != 2 {
		}
		legacy_mapping[names[0]] = names[1]
	}

	return legacy_mapping
}

// the first line of SS test is in format "K: web100_variables_separated_by_space"
func ParseHeader(header string) ([]string, error) {
	web100_vars := strings.Split(header, " ")
	if web100_vars[0] != "K:" {
		return errors.New("Corrupted header")
	}
	mapping := LoadLegacyMapping("legacy_name_mapping.txt")
	var var_names []string
	for index, name := range web100_vars {
		if index == 0 {
			continue
		}
		var_names[index-1] = name
		if mapping[name] != "" {
			var_names[index-1] = mapping[name]
		}
	}
	return var_names
}

func InsertIntoBQ() {

}

func ParseOneLine(snapshot string, ) error {
	value := strings.Split(snapshot, " ")
	if value[0] != "C:" {
		return
	}
        
        for index, val := range value {
          if index == 0 {
            continue
          }
          
        }

}

func (ss *SSParser) ParseAndInsert(meta map[string]bigquery.Value, testName string, rawContent []byte) error {
	time := ParseSSFilename(testName)
}
