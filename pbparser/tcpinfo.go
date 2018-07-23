// Package pbparser comprises parsers that use protobufs in one way or another.
// This is separate from other parsers, because protobuffers incur additional build
// dependencies and overhead.
package pbparser

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"log"

	"github.com/gogo/protobuf/proto"

	tcpinfo "github.com/m-lab/tcp-info/nl-proto"
)

// ReadAll reads and marshals all protobufs from a Reader.
func ReadAll(rdr io.Reader) ([]tcpinfo.TCPDiagnosticsProto, error) {
	var result []tcpinfo.TCPDiagnosticsProto

	byteRdr := bufio.NewReader(rdr)

	for {
		size, err := binary.ReadUvarint(byteRdr)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		log.Println(size)
		buf := make([]byte, size)
		n, err := io.ReadFull(byteRdr, buf)
		if err != nil {
			return nil, err
		}
		if n != len(buf) {
			return nil, errors.New("corrupted protobuf file")
		}

		pb := tcpinfo.TCPDiagnosticsProto{}
		proto.Unmarshal(buf, &pb)
		result = append(result, pb)
	}

	return result, nil
}
