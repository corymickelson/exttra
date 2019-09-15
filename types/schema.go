package types

import (
	"fmt"
	"log"

	"github.com/loanpal-engineering/exttra/pkg"
)

type (
	Signature interface {
	}

	ColumnDefinition struct {
		Index    uint64
		Field    Field
		Aliases  []string
		Name     string
		Unique   bool
		Required bool
	}
	Schema struct {
		dupes   map[string][]int
		headers []string
		indices []string
		columns []*ColumnDefinition
	}

	Opt func(schema *Schema) *Schema
)

// Column
// Adds a new column to the schema with the column Name as Name,
// and types as FieldType
func Column(name string, t Field, required bool, unique ...bool) Opt {
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

// Index a column's values for easy look.
// Indexing does not enforce uniqueness, a value can exist multiple times.
// Indices are mapped with the value being the key, and the value the node's id
// Indexing should not be used on low cardinality columns.
func Index(name string) Opt {
	return func(schema *Schema) *Schema {
		found := false
		for _, v := range schema.columns {
			if v.Name == name {
				found = true
				break
			}
		}
		if !found {
			log.Fatal(fmt.Sprintf("types/schema: column %s not found", name))
		}
		schema.indices = append(schema.indices, name)
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
			pkg.FatalDefect(pkg.Defect{
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

// Check if a column has indexing.
func (s *Schema) Indexed(name string) bool {
	var (
		i = 0
	)
loop:
	if i >= len(s.indices) {
		return false
	}
	if s.indices[i] == name {
		return true
	} else {
		i++
		goto loop
	}
}
