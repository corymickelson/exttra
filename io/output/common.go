package output

import (
	"errors"
	"fmt"
	"log"
	"reflect"

	"github.com/loanpal-engineering/exttra/pkg"
)

type (
	Out interface {
		// Flush the value of an exttra tree to the output type (file, memory)
		// In the case of memory the output is an out parameter passed during initialization.
		// See output.[Mem] for more information
		Flush() error
		base() *output
	}
	addOnArgItem struct {
		col string
		fn  func(args interface{}) *string
	}
	output struct {
		src        pkg.Composer
		addOns     []addOnArgItem
		addOnArgs  map[string]interface{}
		formatters map[uint64]CustomFormatter
	}
	Memory struct {
		output
		out   *[]interface{}
		shape interface{}
		alias map[uint64]reflect.StructField
	}

	FlatFile struct {
		output
		dest   interface{}
		alias  map[uint64]string
		header []string
	}
	Opt func(Out) (Out, error)

	AddOnGenerator func(arg interface{}) *string

	CustomFormatter func(in *string)
)

// Alias a column with a new name. This new name will be used in the output file.
// When writing to a Mem instance, Alias is required for EACH property in the output object
func Alias(col, name string) Opt {
	return func(out Out) (Out, error) {
		var (
			err = errors.New(fmt.Sprintf("output/common: failed to set aliased %s", name))
		)
		target := out.base().src.Find(col)
		if pkg.IsNil(target) {
			// This is not considered a fatal error
			log.Printf("io/output/common: Column %s not found", col)
			return out, nil
		}
		id, _, _ := target.Id()
		switch out.(type) {
		case *Memory:
			if prop, err := out.(*Memory).findField(name); err != nil {
				return nil, err
			} else {
				out.(*Memory).alias[id] = prop
			}
			return out, nil
		case *FlatFile:
			out.(*FlatFile).alias[id] = name
			return out, nil
		default:
			return nil, err
		}
	}
}

// Add a new column to the output. The name will be the header (csv) of the column
// and the value is the result of the AddOnGenerator
func AddOn(name string, generator AddOnGenerator, args interface{}) Opt {
	return func(output Out) (Out, error) {
		base := output.base()
		base.addOns = append(base.addOns, addOnArgItem{fn: generator, col: name})
		base.addOnArgs[name] = args
		return output, nil
	}
}

// Add a custom formatter for a column.
// This formatter is ran AFTER the value is converted to a string.
func Format(col string, formatter CustomFormatter) Opt {
	return func(out Out) (Out, error) {
		target := out.base().src.Find(col)
		if pkg.IsNil(target) {
			log.Printf("output.Alias Column %s not found", col)
			return out, nil
		}
		id, _, _ := target.Id()
		out.base().formatters[id] = formatter
		return out, nil
	}
}
