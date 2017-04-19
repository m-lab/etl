package web100

import (
	"io"
	"io/ioutil"
	"strings"
)

// ParseWeb100Definitions reads all web100 variable definitions from tcpKis and
// returns a mapping from legacy names to canonical names. This mapping is
// necessary for translating variable names in archived web100 snapshots to
// canonical variable names.
func ParseWeb100Definitions(tcpKis io.Reader) (map[string]string, error) {
	var legacyNamesToNewNames map[string]string
	var preferredName string

	data, err := ioutil.ReadAll(tcpKis)
	if err != nil {
		return nil, err
	}

	legacyNamesToNewNames = make(map[string]string)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		if fields[0] == "VariableName:" {
			preferredName = fields[1]
		}

		if fields[0] == "RenameFrom:" {
			for _, legacyName := range fields[1:] {
				legacyNamesToNewNames[legacyName] = preferredName
			}
		}
	}
	return legacyNamesToNewNames, nil
}
