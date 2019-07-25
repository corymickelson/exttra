package types

import (
	"errors"
	"log"

	"github.com/corymickelson/exttra/pkg"
)

type (
	Prop int

	FieldOverride func(*Field) (*Field, error)

	Field struct {
		T         pkg.FieldType
		Nil       *pkg.Nullable
		convert   pkg.FieldLevelConverter
		toString  pkg.StringifyField
		Extension pkg.FieldExtension
	}
)

const (
	Convert Prop = iota
	ToString
)

// Check the raw value for nil
func (f *Field) CheckNil(v interface{}) (*string, error) {
	defNil := ""
	for _, n := range f.Nil.Variants {
		if v == n {
			if f.Nil.Allowed {
				if f.Nil.ReplaceWith != nil {
					return f.Nil.ReplaceWith, nil
				} else {
					return &defNil, nil
				}
			} else {
				return nil, errors.New("nil value found in non-nil field")
			}
		}
	}
	return nil, nil
}

// Convert the converted value to a string representation suitable for csv output
func (f *Field) ToString(v interface{}) *string {
	return f.toString(v)
}

// Convert the input to the type defined in the fields column definition.
// Convert returns (interface, *string, error) where:
// 		interface is the converted value
// 		*string is an explicit null value if the raw value is nil or a permutation of nil
// 		error if the conversion fails
func (f *Field) Convert(in *string) (interface{}, *string, error) {
	explicitNil, err := f.CheckNil(*in)
	if err != nil {
		return nil, nil, err
	}
	if explicitNil != nil {
		return nil, explicitNil, nil
	}
	if f.T == pkg.STRING {
		return *in, nil, nil
	}
	out, err := f.convert(in)
	return out, nil, err
}

// Extend a default converter with additional validation/conversion
func Extend(m Prop, fn interface{}) FieldOverride {
	return func(f *Field) (*Field, error) {
		var err error = nil
		switch m {
		case Convert:
			switch fn.(type) {
			case pkg.FieldExtension:
				f.Extension = fn.(pkg.FieldExtension)
			default:
				err = errors.New("convert must be of type FieldLevelConverter")
			}
		case ToString:
			switch fn.(type) {
			case pkg.FieldExtension:
				f.Extension = fn.(pkg.FieldExtension)
			default:
				err = errors.New("stringify must be of type StringifyField")
			}
		default:

		}
		return f, err
	}
}

// override functions: Convert, ToString.
// This is only used for custom field types.
func Override(m Prop, fn interface{}) FieldOverride {
	return func(f *Field) (*Field, error) {
		var err error = nil
		switch m {
		case Convert:
			switch fn.(type) {
			case pkg.FieldLevelConverter:
				f.convert = fn.(pkg.FieldLevelConverter)
			default:
				err = errors.New("convert must be of type FieldLevelConverter")
			}
		case ToString:
			switch fn.(type) {
			case pkg.StringifyField:
				f.toString = fn.(pkg.StringifyField)
			default:
				err = errors.New("stringify must be of type StringifyField")
			}
		default:

		}
		return f, err
	}
}

// Create a new field.
func NewField(field *Field, nilable *pkg.Nullable, opts ...FieldOverride) (*Field, error) {
	var err error = nil
	if field == nil || nilable == nil {
		return nil, errors.New("a new field requires a Field, and Nullable")
	}
	field.Extension = nil
	defaultNil := ""
	setDefaultNil := false
	for _, v := range nilable.Variants {
		if v == defaultNil {
			setDefaultNil = true
			break
		}
	}
	if !setDefaultNil {
		nilable.Variants = append(nilable.Variants, "")
	}
	field.Nil = nilable
	if field.T == pkg.CUSTOM {
		if len(opts) < 2 {
			log.Fatal("custom fields require opts for convert and stringify functions")
		}
	} else {
		field.toString = SimpleToString
		switch field.T {
		case pkg.INT:
			field.convert = IntConverter
		case pkg.BOOL:
			field.convert = BoolConverter
		case pkg.FLOAT:
			field.convert = FloatConverter
		case pkg.TIMESTAMP:
			fallthrough
		case pkg.DATE:
			field.convert = DateTimeConverter
		case pkg.STRING: // for string types no converter is necessary
			field.convert = nil
		default:
			field.convert = nil
		}
	}
	for _, opt := range opts {
		field, err = opt(field)
		if err != nil {
			pkg.FatalDefect(&pkg.Defect{
				Msg: err.Error(),
			})
		}
	}
	return field, nil
}
