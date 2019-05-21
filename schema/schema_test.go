package schema_test

import (
	"github.com/m-lab/etl/parser"
	"github.com/m-lab/etl/schema"
)

func assertAnnotatable(r *schema.SS) {
	func(parser.Annotatable) {}(r)
}
