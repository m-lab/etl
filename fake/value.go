package fake

import (
	"fmt"
	"math/big"
	"reflect"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
)

func toUploadValue(val interface{}, fs *bigquery.FieldSchema) interface{} {
	if fs.Type == bigquery.TimeFieldType || fs.Type == bigquery.DateTimeFieldType || fs.Type == bigquery.NumericFieldType {
		return toUploadValueReflect(reflect.ValueOf(val), fs)
	}
	return val
}

func toUploadValueReflect(v reflect.Value, fs *bigquery.FieldSchema) interface{} {
	switch fs.Type {
	case bigquery.TimeFieldType:
		if v.Type() == typeOfNullTime {
			return v.Interface()
		}
		return formatUploadValue(v, fs, func(v reflect.Value) string {
			return bigquery.CivilTimeString(v.Interface().(civil.Time))
		})
	case bigquery.DateTimeFieldType:
		if v.Type() == typeOfNullDateTime {
			return v.Interface()
		}
		return formatUploadValue(v, fs, func(v reflect.Value) string {
			return bigquery.CivilDateTimeString(v.Interface().(civil.DateTime))
		})
	case bigquery.NumericFieldType:
		if r, ok := v.Interface().(*big.Rat); ok && r == nil {
			return nil
		}
		return formatUploadValue(v, fs, func(v reflect.Value) string {
			return bigquery.NumericString(v.Interface().(*big.Rat))
		})
	default:
		if !fs.Repeated || v.Len() > 0 {
			return v.Interface()
		}
		// The service treats a null repeated field as an error. Return
		// nil to omit the field entirely.
		return nil
	}
}

func formatUploadValue(v reflect.Value, fs *bigquery.FieldSchema, cvt func(reflect.Value) string) interface{} {
	if !fs.Repeated {
		return cvt(v)
	}
	if v.Len() == 0 {
		return nil
	}
	s := make([]string, v.Len())
	for i := 0; i < v.Len(); i++ {
		s[i] = cvt(v.Index(i))
	}
	return s
}

// structFieldToUploadValue converts a struct field to a value suitable for ValueSaver.Save, using
// the schemaField as a guide.
// structFieldToUploadValue is careful to return a true nil interface{} when needed, so its
// caller can easily identify a nil value.
func structFieldToUploadValue(vfield reflect.Value, schemaField *bigquery.FieldSchema) (interface{}, error) {
	if schemaField.Repeated && (vfield.Kind() != reflect.Slice && vfield.Kind() != reflect.Array) {
		return nil, fmt.Errorf("bigquery: repeated schema field %s requires slice or array, but value has type %s",
			schemaField.Name, vfield.Type())
	}

	// A non-nested field can be represented by its Go value, except for some types.
	if schemaField.Type != bigquery.RecordFieldType {
		return toUploadValueReflect(vfield, schemaField), nil
	}
	// A non-repeated nested field is converted into a map[string]Value.
	if !schemaField.Repeated {
		m, err := structToMap(vfield, schemaField.Schema)
		if err != nil {
			return nil, err
		}
		if m == nil {
			return nil, nil
		}
		return m, nil
	}
	// A repeated nested field is converted into a slice of maps.
	if vfield.Len() == 0 {
		return nil, nil
	}
	var vals []bigquery.Value
	for i := 0; i < vfield.Len(); i++ {
		m, err := structToMap(vfield.Index(i), schemaField.Schema)
		if err != nil {
			return nil, err
		}
		vals = append(vals, m)
	}
	return vals, nil
}

// parseCivilDateTime parses a date-time represented in a BigQuery SQL
// compatible format and returns a civil.DateTime.
func parseCivilDateTime(s string) (civil.DateTime, error) {
	parts := strings.Fields(s)
	if len(parts) != 2 {
		return civil.DateTime{}, fmt.Errorf("bigquery: bad DATETIME value %q", s)
	}
	return civil.ParseDateTime(parts[0] + "T" + parts[1])
}

var StructToMap = structToMap

func structToMap(vstruct reflect.Value, schema bigquery.Schema) (map[string]bigquery.Value, error) {
	if vstruct.Kind() == reflect.Ptr {
		vstruct = vstruct.Elem()
	}
	if !vstruct.IsValid() {
		return nil, nil
	}
	m := map[string]bigquery.Value{}
	if vstruct.Kind() != reflect.Struct {
		return nil, fmt.Errorf("bigquery: type is %s, need struct or struct pointer", vstruct.Type())
	}
	fields, err := fieldCache.Fields(vstruct.Type())
	if err != nil {
		return nil, err
	}
	for _, schemaField := range schema {
		// Look for an exported struct field with the same name as the schema
		// field, ignoring case.
		structField := fields.Match(schemaField.Name)
		if structField == nil {
			continue
		}
		val, err := structFieldToUploadValue(vstruct.FieldByIndex(structField.Index), schemaField)
		if err != nil {
			return nil, err
		}
		// Add the value to the map, unless it is nil.
		if val != nil {
			m[schemaField.Name] = val
		}
	}
	return m, nil
}
