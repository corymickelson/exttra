package output

import (
	"bytes"
	"encoding/csv"
	"github.com/loanpal-engineering/exttra/types"
	"log"
	"os"

	"github.com/loanpal-engineering/exttra/pkg"
)

// Create an output for a root node [pkg.Composer].
// Root node must come from exttra.parser or exttra.view.
// Optional properties:
// 		AddOn: append an additional column where the value is created from the function response
// 		Alias: add a display name for the column specified in the Alias parameter
// Returns a flatFile object. Nothing has been written at this point.
// To write call [Flush]
func Csv(data pkg.Composer, dest interface{}, opts ...Opt) Out {
	i := new(flatFile)
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
		i = ii.(*flatFile)
	}
	return i
}

// Flush
// flushes node to all destinations
// destination files, buffers, and error (if any) are returned
func (i *flatFile) Flush() error {
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
	for ii := range root.Null() {
		if !root.Null()[ii] {
			l++
		}
	}
	columns := make([][]string, l) // iterate over columns
	column := make(chan pkg.Pair)
	sent := 0
	complete := 0
	for id, v := range root.Children() {
		if pkg.IsNil(v) || root.Null()[id] {
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
				rows := i.buildRows(columns)
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
func (i *flatFile) buildRows(cols [][]string) [][]string {
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
	for ii := 0; ii < *length; ii++ {
		row := make([]string, len(cols)+len(i.addOns))
		emptyCols := 0
		for iii := 0; iii < len(cols); iii++ {
			if cols[iii][ii] == "" {
				emptyCols++
			}
			row[iii] = cols[iii][ii]
		}
		if emptyCols == len(cols) {
			continue
		}
		addOnCount := 0
		for n, f := range i.addOns {
			if ii == 0 {
				row[len(cols)+addOnCount] = n
			} else {
				arg := i.addOnArgs[n]
				row[len(cols)+addOnCount] = *f(arg)
			}
			addOnCount++
		}
		rows = append(rows, row)
		// rows[ii] = row
	}
	return rows
}
func (i *flatFile) buildColumn(out chan pkg.Pair, n pkg.Composer, colIdx int) {
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
				if !pkg.IsNil(n.Nullable()) && n.Nullable().ReplaceWith != nil {
					val[row+1] = *n.Nullable().ReplaceWith
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
func (i flatFile) base() *output { return &i.output }
