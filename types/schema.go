package types

import (
	"fmt"

	"github.com/corymickelson/exttra/pkg"
)

type (
	Signature interface {
	}

	ColumnDefinition struct {
		Name     string
		Field    *Field
		Unique   bool
		Aliases  []string
		Required bool
		Index    uint64
	}
	Schema struct {
		columns []*ColumnDefinition
		dupes   map[string][]int
		headers []string
	}

	Opt func(schema *Schema) *Schema
)

// Column
// Adds a new column to the schema with the column Name as Name,
// and types as FieldType
func Column(name string, t *Field, required bool, unique ...bool) Opt {
	return func(schema *Schema) *Schema {
		uni := false
		if len(unique) > 0 {
			uni = unique[0]
		}
		schema.columns = append(schema.columns, &ColumnDefinition{
			Name:     name,
			Field:    t,
			Required: required,
			Unique:   uni,
		})
		return schema
	}
}

// Alias
// If a column may come in with a different Name but
// should map to an existing column use Alias to add to the transform
// a list of values to use for searching a specific column
func Alias(columnName string, name string) Opt {
	return func(schema *Schema) *Schema {
		found := false
		for _, v := range schema.columns {
			if v.Name == columnName {
				(*v).Aliases = append((*v).Aliases, name)
				found = true
				break
			}
		}
		if !found {
			pkg.FatalDefect(&pkg.Defect{
				Msg: fmt.Sprintf("column [ %s ] does not exist on this schema", name),
			})
		}
		return schema

	}
}

// Create a new Schema.
// Build the schema through optional [opts] Column, Alias.
// The schema signature is returned.
// If schema fails a fatal response is thrown
func NewSchema(opts ...Opt) Signature {
	s := new(Schema)
	s.columns = make([]*ColumnDefinition, 0, 10)
	s.dupes = make(map[string][]int)
	s.headers = make([]string, 10)
	for _, opt := range opts {
		s = opt(s)
	}
	return s
}

// Get the column definitions of this schema.
func (s *Schema) Cols() []*ColumnDefinition {
	return s.columns
}
