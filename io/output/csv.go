package output

import (
	"bytes"
	"encoding/csv"
	"log"
	"os"

	"github.com/corymickelson/exttra/pkg"
	"github.com/corymickelson/exttra/types"
)

type (
	CsvOutput struct {
		src        pkg.Composer
		dest       interface{}
		addOns     map[string]func(args interface{}) *string
		addOnArgs  map[string]interface{}
		alias      map[uint64]string
		formatters map[uint64]CustomFormatter
	}

	Opt func(*CsvOutput) (*CsvOutput, error)

	AddOnGenerator func(arg interface{}) *string

	CustomFormatter func(in *string)
)

// Create an output for a root node [pkg.Composer].
// Root node must come from exttra.parser or exttra.view.
// Optional properties:
// 		AddOn: append an additional column where the value is created from the function response
// 		Alias: add a display name for the column specified in the Alias parameter
// Returns a CsvOutput object. Nothing has been written at this point.
// To write call [Flush]
func CsvOut(data pkg.Composer, dest interface{}, opts ...Opt) *CsvOutput {
	i := new(CsvOutput)
	i.src = data
	i.dest = make([]interface{}, 0, 10)
	i.addOns = make(map[string]func(args interface{}) *string)
	i.addOnArgs = make(map[string]interface{})
	i.alias = make(map[uint64]string)
	i.formatters = make(map[uint64]CustomFormatter)
	if pkg.IsNil(dest) {
		log.Fatal("io/output/csv: destination can not be nil")
	}
	i.dest = dest
	for _, o := range opts {
		ii, er := o(i)
		if er != nil {
			log.Fatal(er)
		}
		i = ii
	}
	return i
}

// Alias a column with a new name. This new name will be used in the output file.
func Alias(col, name string) Opt {
	return func(out *CsvOutput) (*CsvOutput, error) {
		target := out.src.Find(col)
		if pkg.IsNil(target) {
			// This is not considered a fatal error
			log.Printf("output.Alias Column %s not found", col)
			return out, nil
		}
		id, _, _ := target.Id()
		out.alias[id] = name
		return out, nil
	}
}

// Add a new column to the output. The name will be the header (csv) of the column
// and the value is the result of the AddOnGenerator
func AddOn(name string, generator AddOnGenerator, args interface{}) Opt {
	return func(output *CsvOutput) (*CsvOutput, error) {
		output.addOns[name] = generator
		output.addOnArgs[name] = args
		return output, nil
	}
}

// Add a custom formatter for a column.
// This formatter is ran AFTER the value is converted to a string.
func Format(col string, formatter CustomFormatter) Opt {
	return func(out *CsvOutput) (*CsvOutput, error) {
		target := out.src.Find(col)
		if pkg.IsNil(target) {
			log.Printf("output.Alias Column %s not found", col)
			return out, nil
		}
		id, _, _ := target.Id()
		out.formatters[id] = formatter
		return out, nil
	}
}

// Flush
// flushes node to all destinations
// destination files, buffers, and error (if any) are returned
func (i *CsvOutput) Flush() error {
	var (
		writer *csv.Writer
	)
	switch i.dest.(type) {
	case string:
		strDest := i.dest.(string)
		if f, err := os.Create(strDest); err != nil {
			log.Fatal(err)
		} else {
			writer = csv.NewWriter(f)
		}
	case *bytes.Buffer:
		bufDest := i.dest.(*bytes.Buffer)
		writer = csv.NewWriter(bufDest)
	default:
		log.Fatal("output.Flush unknown destination type")
	}

	root := i.src
	l := 0
	for ii := range root.Nulls() {
		if !root.Nulls()[ii] {
			l++
		}
	}
	columns := make([][]string, l) // iterate over columns
	column := make(chan pkg.Pair)
	sent := 0
	complete := 0
	for id, v := range root.Children() {
		if pkg.IsNil(v) || root.Nulls()[id] {
			continue
		}
		go i.buildColumn(column, v, sent)
		sent++
	}
	for {
		select {
		case r := <-column:
			complete++
			idx := r.Second.(int)
			colRow := r.First.([]string)
			columns[idx] = colRow
			if sent == complete {
				rows := buildRows(columns, i.addOns, i.addOnArgs)
				err := writer.WriteAll(rows)
				if err != nil {
					pkg.FatalDefect(&pkg.Defect{
						Msg: err.Error(),
					})
				}
				writer.Flush()
				return nil
			}
		}
	}
}
func buildRows(cols [][]string,
	addOns map[string]func(args interface{}) *string,
	addOnArgs map[string]interface{}) [][]string {
	var length *int = nil
	for _, c := range cols {
		if c == nil {
			continue
		}
		if length != nil {
			if len(c) != *length {
				log.Fatal("variable length columns not supported")
			}
		}
		l := len(c)
		length = &l
	}
	if length == nil {
		log.Fatal("output/csv: can not build rows with empty columns")
	}
	rows := make([][]string, 0)
	for i := 0; i < *length; i++ {
		row := make([]string, len(cols)+len(addOns))
		emptyCols := 0
		for ii := 0; ii < len(cols); ii++ {
			if cols[ii][i] == "" {
				emptyCols++
			}
			row[ii] = cols[ii][i]
		}
		if emptyCols == len(cols) {
			continue
		}
		addOnCount := 0
		for n, f := range addOns {
			if i == 0 {
				row[len(cols)+addOnCount] = n
			} else {
				arg := addOnArgs[n]
				row[len(cols)+addOnCount] = *f(arg)
			}
			addOnCount++
		}
		rows = append(rows, row)
		// rows[i] = row
	}
	return rows
}
func (i *CsvOutput) buildColumn(out chan pkg.Pair, n pkg.Composer, colIdx int) {
	val := make([]string, n.Max()+2) // add one row for headers, and one as the Max value(row) must be inclusive, ex. if max = 10, then val[10] must not be out of range.
	id, _, _ := n.Id()
	if alias, ok := i.alias[id]; ok {
		val[0] = alias
	} else {
		val[0] = n.Name()
	}
	var format CustomFormatter
	if f, ok := i.formatters[id]; ok {
		format = f
	}
	excludes := i.src.(pkg.Editor).Excludes()
	for _, v := range n.Children() {
		_, _, row := v.Id()
		if excludes[row] {
			continue
		}

		if uint32(len(val)) < row {
			target := make([]string, len(val)*2)
			copy(target, val)
			val = target
		}
		if row == 0 {
			// all rows are offset by one when written to accommodate headers
			continue
		} else {
			vv := types.SimpleToString(v.Value())
			if vv == nil || *vv == "" {
				if !pkg.IsNil(n.(pkg.Editor).Nullable()) && n.(pkg.Editor).Nullable().ReplaceWith != nil {
					val[row+1] = *n.(pkg.Editor).Nullable().ReplaceWith
				} else {
					val[row+1] = ""
				}
				continue
			} else {
				if format != nil {
					format(vv)
				}
				val[row+1] = *vv
				continue
			}
		}
	}
	out <- pkg.Pair{First: val, Second: colIdx}
}
