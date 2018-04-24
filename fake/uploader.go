package fake

//========================================================================================
// This file contains code pulled from bigquery golang libraries, to emulate the library
// behavior, without hitting the backend.  It also allows examination of the rows that
// are ultimately sent to the service.
//========================================================================================
import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"runtime/debug"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"context"
	bqv2 "google.golang.org/api/bigquery/v2"
)

//---------------------------------------------------------------------------------------
// Stuff from params.go
//---------------------------------------------------------------------------------------
var (
	// See https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp-type.
	timestampFormat = "2006-01-02 15:04:05.999999-07:00"

	// See https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#schema.fields.name
	validFieldName = regexp.MustCompile("^[a-zA-Z_][a-zA-Z0-9_]{0,127}$")
)

func bqTagParser(t reflect.StructTag) (name string, keep bool, other interface{}, err error) {
	if s := t.Get("bigquery"); s != "" {
		if s == "-" {
			return "", false, nil, nil
		}
		if !validFieldName.MatchString(s) {
			return "", false, nil, errInvalidFieldName
		}
		return s, true, nil, nil
	}
	return "", true, nil, nil
}

var fieldCache = NewFieldCache(bqTagParser, nil, nil)

var (
	typeOfDate     = reflect.TypeOf(civil.Date{})
	typeOfTime     = reflect.TypeOf(civil.Time{})
	typeOfDateTime = reflect.TypeOf(civil.DateTime{})
	typeOfGoTime   = reflect.TypeOf(time.Time{})
)

//---------------------------------------------------------------------------------------
// Stuff from schema.go
//---------------------------------------------------------------------------------------

var (
	errNoStruct             = errors.New("bigquery: can only infer schema from struct or pointer to struct")
	errUnsupportedFieldType = errors.New("bigquery: unsupported type of field in struct")
	errInvalidFieldName     = errors.New("bigquery: invalid name of field in struct")
)

var typeOfByteSlice = reflect.TypeOf([]byte{})

var schemaCache Cache

type cacheVal struct {
	schema bigquery.Schema
	err    error
}

func inferSchemaReflectCached(t reflect.Type) (bigquery.Schema, error) {
	cv := schemaCache.Get(t, func() interface{} {
		s, err := inferSchemaReflect(t)
		return cacheVal{s, err}
	}).(cacheVal)
	return cv.schema, cv.err
}

func inferSchemaReflect(t reflect.Type) (bigquery.Schema, error) {
	rec, err := hasRecursiveType(t, nil)
	if err != nil {
		return nil, err
	}
	if rec {
		return nil, fmt.Errorf("bigquery: schema inference for recursive type %s", t)
	}
	return inferStruct(t)
}

func inferStruct(t reflect.Type) (bigquery.Schema, error) {
	switch t.Kind() {
	case reflect.Ptr:
		if t.Elem().Kind() != reflect.Struct {
			return nil, errNoStruct
		}
		t = t.Elem()
		fallthrough

	case reflect.Struct:
		return inferFields(t)
	default:
		return nil, errNoStruct
	}
}

// inferFieldSchema infers the FieldSchema for a Go type
func inferFieldSchema(rt reflect.Type) (*bigquery.FieldSchema, error) {
	switch rt {
	case typeOfByteSlice:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.BytesFieldType}, nil
	case typeOfGoTime:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.TimestampFieldType}, nil
	case typeOfDate:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.DateFieldType}, nil
	case typeOfTime:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.TimeFieldType}, nil
	case typeOfDateTime:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.DateTimeFieldType}, nil
	}
	if isSupportedIntType(rt) {
		return &bigquery.FieldSchema{Required: true, Type: bigquery.IntegerFieldType}, nil
	}
	switch rt.Kind() {
	case reflect.Slice, reflect.Array:
		et := rt.Elem()
		if et != typeOfByteSlice && (et.Kind() == reflect.Slice || et.Kind() == reflect.Array) {
			// Multi dimensional slices/arrays are not supported by BigQuery
			return nil, errUnsupportedFieldType
		}

		f, err := inferFieldSchema(et)
		if err != nil {
			return nil, err
		}
		f.Repeated = true
		f.Required = false
		return f, nil
	case reflect.Struct, reflect.Ptr:
		nested, err := inferStruct(rt)
		if err != nil {
			return nil, err
		}
		return &bigquery.FieldSchema{Required: true, Type: bigquery.RecordFieldType, Schema: nested}, nil
	case reflect.String:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.StringFieldType}, nil
	case reflect.Bool:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.BooleanFieldType}, nil
	case reflect.Float32, reflect.Float64:
		return &bigquery.FieldSchema{Required: true, Type: bigquery.FloatFieldType}, nil
	default:
		return nil, errUnsupportedFieldType
	}
}

// inferFields extracts all exported field types from struct type.
func inferFields(rt reflect.Type) (bigquery.Schema, error) {
	var s bigquery.Schema
	fields, err := fieldCache.Fields(rt)
	if err != nil {
		return nil, err
	}
	for _, field := range fields {
		f, err := inferFieldSchema(field.Type)
		if err != nil {
			return nil, err
		}
		f.Name = field.Name
		s = append(s, f)
	}
	return s, nil
}

// isSupportedIntType reports whether t can be properly represented by the
// BigQuery INTEGER/INT64 type.
func isSupportedIntType(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int,
		reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return true
	default:
		return false
	}
}

// typeList is a linked list of reflect.Types.
type typeList struct {
	t    reflect.Type
	next *typeList
}

func (l *typeList) has(t reflect.Type) bool {
	for l != nil {
		if l.t == t {
			return true
		}
		l = l.next
	}
	return false
}

// hasRecursiveType reports whether t or any type inside t refers to itself, directly or indirectly,
// via exported fields. (Schema inference ignores unexported fields.)
func hasRecursiveType(t reflect.Type, seen *typeList) (bool, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false, nil
	}
	if seen.has(t) {
		return true, nil
	}
	fields, err := fieldCache.Fields(t)
	if err != nil {
		return false, err
	}
	seen = &typeList{t, seen}
	// Because seen is a linked list, additions to it from one field's
	// recursive call will not affect the value for subsequent fields' calls.
	for _, field := range fields {
		ok, err := hasRecursiveType(field.Type, seen)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

//---------------------------------------------------------------------------------------
// Stuff from uploader.go
//---------------------------------------------------------------------------------------

// This is an fake for Uploader, for use in debugging, and tests.
// See bigquery.Uploader for field info.
type FakeUploader struct {
	t                   *bigquery.Table
	SkipInvalidRows     bool
	IgnoreUnknownValues bool
	TableTemplateSuffix string

	Rows    []*InsertionRow // Most recently inserted rows, for testing/debugging.
	Request *bqv2.TableDataInsertAllRequest
	// Set this with SetErr to return an error.  Error is cleared on each call.
	Err       error
	CallCount int // Number of times Put is called.
}

func (up *FakeUploader) SetErr(err error) {
	up.Err = err
}

func NewFakeUploader() *FakeUploader {
	return new(FakeUploader)
}

// Put uploads one or more rows to the BigQuery service.
//
// If src is ValueSaver, then its Save method is called to produce a row for uploading.
//
// If src is a struct or pointer to a struct, then a schema is inferred from it
// and used to create a StructSaver. The InsertID of the StructSaver will be
// empty.
//
// If src is a slice of ValueSavers, structs, or struct pointers, then each
// element of the slice is treated as above, and multiple rows are uploaded.
//
// Put returns a PutMultiError if one or more rows failed to be uploaded.
// The PutMultiError contains a RowInsertionError for each failed row.
//
// Put will retry on temporary errors (see
// https://cloud.google.com/bigquery/troubleshooting-errors). This can result
// in duplicate rows if you do not use insert IDs. Also, if the error persists,
// the call will run indefinitely. Pass a context with a timeout to prevent
// hanging calls.
func (u *FakeUploader) Put(ctx context.Context, src interface{}) error {
	u.CallCount++
	if u.Err != nil {
		t := u.Err
		u.Err = nil
		return t
	}

	savers, err := valueSavers(src)
	if err != nil {
		log.Printf("Put: %v\n", err)
		log.Printf("src: %v\n", src)
		debug.PrintStack()
		return err
	}
	return u.putMulti(ctx, savers)
}

func valueSavers(src interface{}) ([]bigquery.ValueSaver, error) {
	saver, ok, err := toValueSaver(src)
	if err != nil {
		return nil, err
	}
	if ok {
		return []bigquery.ValueSaver{saver}, nil
	}
	srcVal := reflect.ValueOf(src)
	if srcVal.Kind() != reflect.Slice {
		return nil, fmt.Errorf("%T is not a ValueSaver, struct, struct pointer, or slice", src)

	}
	var savers []bigquery.ValueSaver
	for i := 0; i < srcVal.Len(); i++ {
		s := srcVal.Index(i).Interface()
		saver, ok, err := toValueSaver(s)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("src[%d] has type %T, which is not a ValueSaver, struct or struct pointer", i, s)
		}
		savers = append(savers, saver)
	}
	return savers, nil
}

// Make a ValueSaver from x, which must implement ValueSaver already
// or be a struct or pointer to struct.
func toValueSaver(x interface{}) (bigquery.ValueSaver, bool, error) {
	if saver, ok := x.(bigquery.ValueSaver); ok {
		return saver, ok, nil
	}
	v := reflect.ValueOf(x)
	// Support Put with []interface{}
	if v.Kind() == reflect.Interface {
		v = v.Elem()
	}
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, false, nil
	}
	schema, err := inferSchemaReflect(v.Type())
	if err != nil {
		return nil, false, err
	}
	return &bigquery.StructSaver{Struct: x, Schema: schema}, true, nil
}

func (u *FakeUploader) putMulti(ctx context.Context, src []bigquery.ValueSaver) error {
	var rows []*InsertionRow
	for _, saver := range src {
		row, insertID, err := saver.Save()
		if err != nil {
			log.Printf("%v\n", err)
			debug.PrintStack()
			return err
		}
		rows = append(rows, &InsertionRow{InsertID: insertID, Row: row})
	}

	u.Rows = rows

	// Substitute for service call.
	var err error
	u.Request, err = insertRows(rows)
	return err
}

// An InsertionRow represents a row of data to be inserted into a table.
type InsertionRow struct {
	// If InsertID is non-empty, BigQuery will use it to de-duplicate insertions of
	// this row on a best-effort basis.
	InsertID string
	// The data to be inserted, represented as a map from field name to Value.
	Row map[string]bigquery.Value
}

//---------------------------------------------------------------------------------------
// Stuff from service.go
//---------------------------------------------------------------------------------------
func insertRows(rows []*InsertionRow) (*bqv2.TableDataInsertAllRequest, error) {
	req := &bqv2.TableDataInsertAllRequest{}
	for _, row := range rows {
		m := make(map[string]bqv2.JsonValue)
		for k, v := range row.Row {
			m[k] = bqv2.JsonValue(v)
		}
		req.Rows = append(req.Rows, &bqv2.TableDataInsertAllRequestRows{
			InsertId: row.InsertID,
			Json:     m,
		})
	}
	// Truncated here, because the remainder hits the backend.
	return req, nil
}
