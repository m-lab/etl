package fake

/*
// Schema describes the fields in a table or query result.
type Schema []*FieldSchema

type FieldSchema struct {
	// The field name.
	// Must contain only letters (a-z, A-Z), numbers (0-9), or underscores (_),
	// and must start with a letter or underscore.
	// The maximum length is 128 characters.
	Name string

	// A description of the field. The maximum length is 16,384 characters.
	Description string

	// Whether the field may contain multiple values.
	Repeated bool
	// Whether the field is required.  Ignored if Repeated is true.
	Required bool

	// The field data type.  If Type is Record, then this field contains a nested schema,
	// which is described by Schema.
	Type FieldType
	// Describes the nested schema if Type is set to Record.
	Schema Schema
}

func (fs *FieldSchema) toBQ() *bqv2.TableFieldSchema {
	tfs := &bqv2.TableFieldSchema{
		Description: fs.Description,
		Name:        fs.Name,
		Type:        string(fs.Type),
	}

	if fs.Repeated {
		tfs.Mode = "REPEATED"
	} else if fs.Required {
		tfs.Mode = "REQUIRED"
	} // else leave as default, which is interpreted as NULLABLE.

	for _, f := range fs.Schema {
		tfs.Fields = append(tfs.Fields, f.toBQ())
	}

	return tfs
}

func (s Schema) toBQ() *bqv2.TableSchema {
	var fields []*bqv2.TableFieldSchema
	for _, f := range s {
		fields = append(fields, f.toBQ())
	}
	return &bqv2.TableSchema{Fields: fields}
}

func bqToFieldSchema(tfs *bqv2.TableFieldSchema) *FieldSchema {
	fs := &FieldSchema{
		Description: tfs.Description,
		Name:        tfs.Name,
		Repeated:    tfs.Mode == "REPEATED",
		Required:    tfs.Mode == "REQUIRED",
		Type:        FieldType(tfs.Type),
	}

	for _, f := range tfs.Fields {
		fs.Schema = append(fs.Schema, bqToFieldSchema(f))
	}
	return fs
}

func bqToSchema(ts *bqv2.TableSchema) Schema {
	if ts == nil {
		return nil
	}
	var s Schema
	for _, f := range ts.Fields {
		s = append(s, bqToFieldSchema(f))
	}
	return s
}

type FieldType string

const (
	StringFieldType    FieldType = "STRING"
	BytesFieldType     FieldType = "BYTES"
	IntegerFieldType   FieldType = "INTEGER"
	FloatFieldType     FieldType = "FLOAT"
	BooleanFieldType   FieldType = "BOOLEAN"
	TimestampFieldType FieldType = "TIMESTAMP"
	RecordFieldType    FieldType = "RECORD"
	DateFieldType      FieldType = "DATE"
	TimeFieldType      FieldType = "TIME"
	DateTimeFieldType  FieldType = "DATETIME"
	NumericFieldType   FieldType = "NUMERIC"
)
*/
