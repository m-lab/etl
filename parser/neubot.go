package parser

import (
	//"github.com/m-lab/etl/bq"
	"github.com/m-lab/etl/etl"
	//"github.com/m-lab/etl/metrics"
	//"github.com/m-lab/etl/schema"
	//"github.com/m-lab/etl/web100"
)

// Neubot parser
// -------------
//
// Here we define the Neubot parser structure and we define the functions
// required to fully implement the etl.Inserter interface.

// NeubotParser is a parser for Neubot data.
type NeubotParser struct {
	inserter etl.Inserter
	etl.RowStats
}

// NewNeubotParser returns a new Neubot parser.
func NewNeubotParser(inserter etl.Inserter) *NeubotParser {
	return &NeubotParser{
		inserter: inserter,
		RowStats: inserter, // Will provide the RowStats interface.
	}
}

// TODO(bassosimone): report to Greg that at this point is not fully clear to
// me what are exactly the "methods" I should "override". Even though there is
// a clear indication in the NDT code, I still would find it more easy if the
// methods to override where clearly indicated perhaps in a section of the file?

/*
// TaskError returns non-nil if more than 10% of row inserts failed.
func (n *NeubotParser) TaskError() error {
	if n.inserter.Committed() < 10 * m.inserter.Failed() {
		log.Printf("Watning: high row insert errors: %d / %d\n",
			n.inserter.Accepted(), n.inserter.Failed())
		return errors.New("too many insertion failures")
	}
	return nil
}

func (n *NeubotParser) Flush() error {
	// TODO(bassosimone): the original code is using n.timestamp to
	// decide whether to process a group. Understand what I should do
	// in this code to properly mimic that behavior.
	return n.inserter.Flush()
}

// TODO(bassosimone): understand what is the purpose of this method. Unclear
// for now but probably it will become clear as I use this code.
func (n *NeubotParser) TableName() string {
	return n.inserter.TableBase()
}

// TODO(bassosimone): understand
func (n *NeubotParser) FullTableName() string {
	return n.inserter.FullTableName()
}

// TODO(bassosimone): understantd
func (n *NeubotParser) IsParsable(testName string, data []byte) (string, bool) {
	// TODO(bassosimone): implement this function
	return "unknown", false
}
*/
