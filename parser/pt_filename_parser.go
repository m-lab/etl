// Parse PT filename like 20170320T23:53:10Z-98.162.212.214-53849-64.86.132.75-42677.paris
package parser

import (
	"strings"
)

type FileName struct {
	name string
}

// GetLocalIP parse the filename and return IP.
func (f *FileName) GetIPTuple() (string, string, string, string) {
	firstIPStart := strings.IndexByte(f.name, '-')
	first_segment := f.name[firstIPStart+1 : len(f.name)]
	firstPortStart := strings.IndexByte(first_segment, '-')
	second_segment := first_segment[firstPortStart+1 : len(first_segment)]
	secondIPStart := strings.IndexByte(second_segment, '-')
	third_segment := second_segment[secondIPStart+1 : len(second_segment)]
	secondPortStart := strings.IndexByte(third_segment, '-')
	secondPortEnd := strings.LastIndexByte(third_segment, '.')
	return first_segment[0:firstPortStart], second_segment[0:secondIPStart], third_segment[0:secondPortStart], third_segment[secondPortStart+1 : secondPortEnd]
}

func (f *FileName) GetDate() string {
	return f.name[0:8]
}

type FileNameParser interface {
	GetIPTuple()
	GetDate()
}
