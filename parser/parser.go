package parser

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"exttra/internal/data"
	"exttra/internal/defect"
	"exttra/io/input"
	"exttra/pkg"
	"exttra/types"
	"github.com/pkg/errors"
)

type (
	Parser interface {
		Validate(*int) error
		Parse() error
	}
	parser struct {
		data      pkg.Node
		input     input.Input
		headerIdx uint32
		primary   []uint64
		keys      map[uint64]map[string]uint8
	}
)

// Create a new parser object for the input provided.
func NewParser(in *input.Input) *parser {
	if in == nil {
		return nil
	}
	i := new(parser)
	i.input = *in
	i.primary = make([]uint64, 0, 10)
	i.keys = make(map[uint64]map[string]uint8)
	return i
}
func (p *parser) readRow() *[]string {
	reader := p.input.GetReader().(*csv.Reader)
	currentRow := &p.headerIdx
	var (
		err error = nil
	)
	row, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			if len(p.primary) > 0 {
				// p.fillInDefects()
			}
			return nil
		} else {
			log.Fatal(err.Error())
		}
	}
	*currentRow++
	process := false
	for _, v := range row {
		if len(v) > 0 {
			process = true
			break
		}
	}
	if !process || len(row) == 0 {
		if len(p.primary) > 0 {
			// p.fillInDefects()
		}
		return nil
	}
	return &row
}
func (p *parser) colDef(colIdx uint64, defs []*types.ColumnDefinition) *types.ColumnDefinition {
	i := 0
loop:
	if len(defs) > i {
		if c := defs[i]; c.Index == colIdx {
			return c
		}
		i++
		goto loop
	}
	return nil
}
func (p *parser) parseRow(row *[]string) error {
	currentRow := &p.headerIdx
	colRow := make([]pkg.Node, 0)
	for i, field := range *row {
		var (
			id     uint64
			n      pkg.Node                = nil
			col    pkg.Node                = nil
			err    error                   = nil
			colDef *types.ColumnDefinition = nil
			ok                             = false
		)
		colIdx := uint64(i + 1)
		d := &defect.Defect{
			Col: strconv.Itoa(int(colIdx)),
			Row: strconv.Itoa(int(*currentRow)),
		}
		if col, ok = p.data.Children()[colIdx]; !ok {
			continue
		} else {
			colDef = p.colDef(colIdx, p.input.GetSchema().(*types.Schema).Cols())
			if colDef == nil {
				return errors.New(fmt.Sprint("parser/Parse: schema column definition not found for index %l", colIdx))
			}

			id = pkg.GenNodeId(uint32(colIdx), *currentRow)
			nilNode, _ := data.NewNode(&id, data.V(nil))
			if item, en, err := colDef.Field.Convert(&field); err != nil {
				d.Msg = err.Error()
				n = nilNode
			} else if en != nil { // todo: move explicit null to output
				n, err = data.NewNode(&id, data.V(nil))
			} else {
				if colDef.Field.Extension != nil {
					if item, err = colDef.Field.Extension(item); err != nil || item == nil {
						if err != nil {
							d.Msg = err.Error()
						}
						n = nilNode
						if err = col.Add(n, true); err != nil {
							d.Msg = err.Error()
						}
						colRow = append(colRow, n)
						if d.Msg != "" {
							defect.LogDefect(d)
						}
						continue
					}
				}

				switch colDef.Field.T {
				case pkg.TIMESTAMP:
					fallthrough
				case pkg.DATE:
					switch item.(type) {
					case time.Time:
						n, err = data.NewNode(&id, data.V(item.(time.Time)))
					default:
						d.Msg = "parser/parse: time was expected"
						defect.LogDefect(d)
						n = nilNode
					}
				case pkg.FLOAT:
					switch item.(type) {
					case float64:
						n, err = data.NewNode(&id, data.V(item.(float64)))
					default:
						d.Msg = "parser/parse: float was expected"
						defect.LogDefect(d)
						n = nilNode
					}
				case pkg.CUSTOM:
					fallthrough
				case pkg.STRING:
					switch item.(type) {
					case string:
						n, err = data.NewNode(&id, data.V(item.(string)))
					default:
						d.Msg = "parser/parse: string was expected"
						defect.LogDefect(d)
						n = nilNode
					}
				case pkg.BOOL:
					switch item.(type) {
					case bool:
						n, err = data.NewNode(&id, data.V(item.(bool)))
					default:
						d.Msg = "parser/parse: bool was expected"
						defect.LogDefect(d)
						n = nilNode
					}
				case pkg.INT:
					switch item.(type) {
					case int:
						n, err = data.NewNode(&id, data.V(item.(int)))
					default:
						d.Msg = "parser/parse: bool was expected"
						defect.LogDefect(d)
						n = nilNode
					}
				default:
					d.Msg = "parser/parse: type not defined by pkg.FieldType"
					defect.LogDefect(d)
					n = nilNode
				}
			}
			if n == nil {
				log.Fatal("parser/parser: nil node")
			}
			if err = col.Add(n, n.Value() == nil); err != nil {
				d.Msg = err.Error()
			}
			colRow = append(colRow, n)
			if d.Msg != "" {
				defect.LogDefect(d)
			}
		}
	}
	for i := range colRow {
		if i == 0 {
			colRow[i].Prev(nil)
			colRow[i].Next(colRow[i+1])
			continue
		} else if i == len(colRow)-1 {
			colRow[i].Next(nil)
			colRow[i].Prev(colRow[i-1])
		} else {
			colRow[i].Next(colRow[i+1])
			colRow[i].Prev(colRow[i-1])
		}
	}
	return nil

}
func (p *parser) keyed(row *[]string, rowIdx *uint64) error {
	for _, pi := range p.primary {
		var (
			col    pkg.Node = nil
			colIdx uint32   = 0
		)
		col = p.data.FindById(pi + 1)
		if pkg.IsNil(col) {
			return errors.New("parser/parser: primary key column not found")
		}
		_, colIdx, _ = col.Id()
		candidate := strings.TrimSpace((*row)[colIdx-1])
		_, exists := p.keys[uint64(colIdx-1)][candidate]
		if exists {
			defect.LogDefect(&defect.Defect{
				Msg: fmt.Sprintf("Duplicate id [%s]", candidate),
				Row: strconv.Itoa(int(*rowIdx)),
				Col: strconv.Itoa(int(colIdx - 1)),
			})
			p.keys[uint64(colIdx-1)][candidate]++
			col.(pkg.NodeModifier).Toggle(pkg.GenNodeId(colIdx, uint32(*rowIdx)))
		} else {
			p.keys[uint64(colIdx-1)] = make(map[string]uint8)
			p.keys[uint64(colIdx-1)][candidate] = 0
		}
	}
	return nil
}

// Parse the body of the file.
// On success a Node is returned, this node is the root node of the parse tree.
// Otherwise an error is returned
func (p *parser) Parse() (pkg.Node, error) {
	ri := &p.headerIdx
parse:
	var (
		err error = nil
	)
	if row := p.readRow(); row != nil {
		if err = p.parseRow(row); err != nil {
			return nil, err
		}
		if len(p.primary) > 0 {
			k := uint64(*ri)
			if err = p.keyed(row, &k); err != nil {
				return nil, err
			}
		}
	} else {
		return p.data, nil
	}
	err = nil
	goto parse
}

// Validate the input's schema against the file.
// If validation fails an error is returned
func (p *parser) Validate(index *uint32) error {
	var (
		currentRow = &p.headerIdx
		reader     = p.input.GetReader().(*csv.Reader)
		def        = p.input.GetSchema().(*types.Schema)
		headers    []string
		i          uint64
		field      string
		hi         uint32 = 0
	)

	if index != nil {
		hi = *index
	} else {
		hi = 0
	}

	for {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
		}
		if *currentRow < hi {
			*currentRow++
			continue
		}
		headers = row
		break
	}
	// hash headers for dupes check
	hHeaders := map[string]int8{}
	p.data, _ = data.NewNode(nil) // root node
	// required headers count, used to check all required fields are found
	reqHeadCount := 0
	// iterate over header row
	// if header is part of schema signature (def) create a new node

	for i_, field_ := range headers {
		i = uint64(i_)
		field = field_
		var col *types.ColumnDefinition
		// Add field to header hash for dupe checking
		dupe := false
		// exit used to break out from inner loop
		exit := false
		if hh, exists := hHeaders[field]; exists {
			hh++
			dupe = true
		} else {
			hHeaders[field] = 0
		}
		for _, c := range def.Cols() {
			if field == c.Name {
				col = c
				break
			}
			for _, a := range c.Aliases {
				if field == a {
					col = c
					exit = true
					break
				}
			}
			if exit {
				break
			}
		}
		if col == nil {
			continue
		}
		if col.Required {
			reqHeadCount++
		}
		if col.Required && dupe {
			// todo: how to handle required fields that are duplicated
			return nil
		}
		if col.Unique {
			p.primary = append(p.primary, i)
		}
		col.Index = i + 1
		cid := i + 1
		if n, err := data.NewNode(&cid,
			data.Nullable(col.Field.Nil.Allowed),
			data.Name(field),
			data.Type(&col.Field.T)); err != nil {
			log.Fatalf(err.Error())
		} else if err = p.data.Add(n, false); err != nil {
			log.Fatalf(err.Error())
		}
	}
	cc := 0
	for _, l := range p.data.Children() {
		if l != nil {
			cc++
		}
	}
	if reqHeadCount != cc {
		// todo: missing column send back to sender
		defect.FatalDefect(&defect.Defect{
			Msg: "Missing required column(s)",
		})
	}
	return nil
}

// If a primary key is defined on the input,
// iterate defects column and row to get the key
// to each defect
// todo: this can be simplified by using the nodes Next/Prev method to move horizontally over the row
func (p *parser) fillInDefects() {
	d := defect.New()
	defs := d.Coll()
	if len(p.primary) > 0 {
		for _, colIdx := range p.primary {
			col := p.data.FindById(colIdx + 1)
			if pkg.IsNil(col) {
				continue
			}
			d.(*defect.Defects).Headers = append(d.(*defect.Defects).Headers, col.Name())
		}
		for _, v := range defs {
			if v.Keys == nil {
				v.Keys = make(map[string]string)
			}
			r := v.Row
			if r == "" {
				continue
			}
			rowIdx, err := strconv.Atoi(r)
			if err != nil {
				continue
			}
			for _, pi := range p.primary {
				col := p.data.FindById(pi + 1)
				if pkg.IsNil(col) {
					continue
				}
				id := uint64((pi+1)<<32) | uint64(rowIdx+1)
				row := col.FindById(id)
				if row == nil {
					continue
				}
				v.Keys[col.Name()] = row.Value().(string)
			}
		}
	}
}
