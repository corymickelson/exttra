package parser

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/loanpal-engineering/exttra/data"
	"github.com/loanpal-engineering/exttra/io/input"
	"github.com/loanpal-engineering/exttra/pkg"
	"github.com/loanpal-engineering/exttra/types"
	"github.com/pkg/errors"
)

type (
	Parser interface {
		Validate(*int) error
		Parse() error
	}
	parser struct {
		data      pkg.Composer
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
func (p *parser) readRow() []string {
	reader := p.input.GetReader().(*csv.Reader)
	currentRow := &p.headerIdx
	var (
		err error = nil
	)
	row, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			if len(p.primary) > 0 {
				p.fillInDefects()
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
			p.fillInDefects()
		}
		return nil
	}
	return row
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
func (p *parser) parseRow(row []string) error {
	currentRow := &p.headerIdx
	colRow := make([]pkg.Composer, len(*p.data.Children()))
	offset := 0
	for i, field := range row {
		var (
			id     uint64
			n      pkg.Composer            = nil
			col    pkg.Composer            = nil
			err    error                   = nil
			colDef *types.ColumnDefinition = nil
		)
		colIdx := uint64(i)
		d := pkg.Defect{
			Col: i,
			Row: int(*currentRow),
		}
		colId := pkg.GenNodeId(uint32(colIdx), uint32(0))
		if col = p.data.FindById(colId); col == nil {
			offset++
			continue
		} else {
			p.input.GetSchema().(*types.Schema).Cols()
			colDef = p.colDef(colId, p.input.GetSchema().(*types.Schema).Cols())
			if colDef == nil {
				return errors.New(fmt.Sprint("parser/Parse: schema column definition not found for index %l", colIdx))
			}

			id = pkg.GenNodeId(uint32(colIdx), *currentRow)
			nilNode, _ := data.NewNode(&id)
			if item, en, err := colDef.Field.Convert(&field); err != nil {
				d.Msg = err.Error()
				n = nilNode
			} else if en != nil {
				n, err = data.NewNode(&id)
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
						colRow[i-offset] = n
						if d.Msg != "" {
							pkg.LogDefect(d)
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
						n = nilNode
					}
				case pkg.FLOAT32:
					switch item.(type) {
					case float32:
						n, err = data.NewNode(&id, data.V(item.(float32)))
					default:
						d.Msg = "parser/parse: float32 was expected"
						n = nilNode
					}
				case pkg.FLOAT:
					fallthrough
				case pkg.FLOAT64:
					switch item.(type) {
					case float64:
						n, err = data.NewNode(&id, data.V(item.(float64)))
					default:
						d.Msg = "parser/parse: float64 was expected"
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
						n = nilNode
					}
				case pkg.BOOL:
					switch item.(type) {
					case bool:
						n, err = data.NewNode(&id, data.V(item.(bool)))
					default:
						d.Msg = "parser/parse: bool was expected"
						n = nilNode
					}
				case pkg.UINT:
					fallthrough
				case pkg.UINT64:
					switch item.(type) {
					case uint64:
						n, err = data.NewNode(&id, data.V(item.(uint64)))
					default:
						d.Msg = "parser/parse: uint64 was expected"
						n = nilNode
					}
				case pkg.INT:
					fallthrough
				case pkg.INT64:
					switch item.(type) {
					case int64:
						n, err = data.NewNode(&id, data.V(item.(int64)))
					default:
						d.Msg = "parser/parse: int64 was expected"
						n = nilNode
					}
				case pkg.UINT32:
					switch item.(type) {
					case uint32:
						n, err = data.NewNode(&id, data.V(item.(uint32)))
					default:
						d.Msg = "parser/parse: uint32 was expected"
						n = nilNode
					}
				case pkg.INT32:
					switch item.(type) {
					case int32:
						n, err = data.NewNode(&id, data.V(item.(int32)))
					default:
						d.Msg = "parser/parse: int32 was expected"
						n = nilNode
					}
				case pkg.UINT16:
					switch item.(type) {
					case uint16:
						n, err = data.NewNode(&id, data.V(item.(uint16)))
					default:
						d.Msg = "parser/parse: uint32 was expected"
						n = nilNode
					}
				case pkg.INT16:
					switch item.(type) {
					case int16:
						n, err = data.NewNode(&id, data.V(item.(int16)))
					default:
						d.Msg = "parser/parse: int16 was expected"
						n = nilNode
					}
				case pkg.UINT8:
					switch item.(type) {
					case uint8:
						n, err = data.NewNode(&id, data.V(item.(uint8)))
					default:
						d.Msg = "parser/parse: uint8 was expected"
						n = nilNode
					}
				case pkg.INT8:
					switch item.(type) {
					case int8:
						n, err = data.NewNode(&id, data.V(item.(int8)))
					default:
						d.Msg = "parser/parse: int8 was expected"
						n = nilNode
					}
				default:
					d.Msg = "parser/parse: type not defined by pkg.FieldType"
					n = nilNode
				}
			}
			if n == nil {
				log.Fatal("parser/parser: nil node")
			}
			if d.Msg != "" {
				pkg.LogDefect(d)
			}
			if err = col.Add(n, n.Value() == nil); err != nil {
				d.Msg = err.Error()
				pkg.FatalDefect(d)
			}
			colRow[i-offset] = n
		}
	}
	return p.linkRow(colRow)
}
func (p *parser) linkRow(row []pkg.Composer) error {
	var (
		i         = 0
		err error = nil
	)
loop:
	if i > len(row) {
		return err
	}
	if i == 0 {
		(row)[i].Prev(nil)
		(row)[i].Next((row)[i+1])
		i++
		goto loop
	} else if i == len(row)-1 {
		(row)[i].Next(nil)
		(row)[i].Prev((row)[i-1])
		return err
	} else {
		(row)[i].Next((row)[i+1])
		(row)[i].Prev((row)[i-1])
		i++
		goto loop
	}
}
func (p *parser) keyed(row []string, rowIdx *uint64) error {
	for _, pi := range p.primary {
		var (
			col    pkg.Composer = nil
			colIdx uint32       = 0
		)
		col = p.data.FindById(pi)
		if pkg.IsNil(col) {
			return errors.New("parser/parser: primary key column not found")
		}
		_, colIdx, _ = col.Id()
		if _, exists := p.keys[uint64(colIdx)]; !exists {
			colKeyMap := make(map[string]uint8)
			p.keys[uint64(colIdx)] = colKeyMap
		}
		candidate := strings.TrimSpace((row)[colIdx])
		_, exists := p.keys[uint64(colIdx)][candidate]
		if exists {
			pkg.LogDefect(pkg.Defect{
				Msg: fmt.Sprintf("Duplicate id [%s]", candidate),
				Row: int(*rowIdx),
				Col: int(colIdx),
			})
			p.keys[uint64(colIdx)][candidate]++
			col.(pkg.Editor).Toggle(pkg.GenNodeId(colIdx, uint32(*rowIdx)), true)
		} else {
			p.keys[uint64(colIdx)][candidate] = 0
		}
	}
	return nil
}

// Parse the body of the file.
// The root node of the parse tree is returned.
// Use this node for writing to an output, or creating a new view of the data.
// See [output] and [view]
func (p *parser) Parse() (pkg.Composer, error) {
parse:
	var (
		err error = nil
	)
	if row := p.readRow(); row != nil {
		if err = p.parseRow(row); err != nil {
			return nil, err
		}
		if len(p.primary) > 0 {
			k := uint64(p.headerIdx)
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
		headers    []string
		dupes             = make([]string, 0)
		currentRow        = &p.headerIdx
		reader            = p.input.GetReader().(*csv.Reader)
		def               = p.input.GetSchema().(*types.Schema)
		hi         uint32 = 0
		colRow            = make([]pkg.Composer, 0)
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

	for i, field := range headers {
		field = strings.TrimSpace(field)
		if field == "" {
			break
		}
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
					field = c.Name
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
			dupes = append(dupes, field)
		} else if dupe && !col.Required {
			// If this is a duplicate row AND  the row is not required
			// The parser will use the first instance of the column found
			continue
		}
		id := pkg.GenNodeId(uint32(i), 0)
		if col.Unique {
			p.primary = append(p.primary, id)
		}
		col.Index = id
		opts := []data.Opt{
			data.Nullable(col.Field.Nil),
			data.Name(field),
			data.Type(&col.Field.T),
		}
		if def.Indexed(field) {
			opts = append(opts, data.Index(true))
		}
		if n, err := data.NewNode(&id, opts...); err != nil {
			log.Fatalf(err.Error())
		} else if err = p.data.Add(n, false); err != nil {
			log.Fatalf(err.Error())
		} else {
			colRow = append(colRow, n)
		}

	}
	if len(dupes) > 0 {
		return errors.New(fmt.Sprintf("parser/parser: duplicate column(s) %s found= ", strings.Join(dupes, ",")))
	}
	cc := 0
	for _, l := range *p.data.Children() {
		if l != nil {
			cc++
		}
	}
	if reqHeadCount > cc {
		// todo: missing column send back to sender
		pkg.FatalDefect(pkg.Defect{
			Msg: "Missing required column(s)",
		})
	}
	return p.linkRow(colRow)
}

// If a primary key is defined on the input,
// iterate pkg. column and row to get the key
// to each defect
// todo: this can be simplified by using the nodes Next/Prev method to move horizontally over the row
func (p *parser) fillInDefects() {
	d := pkg.NewDC()
	defs := d.Coll()
	if len(p.primary) > 0 {
		for _, colIdx := range p.primary {
			col := p.data.FindById(colIdx)
			if pkg.IsNil(col) {
				continue
			}
			d.(*pkg.Defects).Headers = append(d.(*pkg.Defects).Headers, col.Name())
		}
		for i, v := range *defs {
			if v.Keys == nil {
				(*defs)[i].Keys = make(map[string]string)
			}
			rowIdx := v.Row
			if rowIdx == -1 {
				continue
			}

			for _, pi := range p.primary {
				col := p.data.FindById(pi)
				_, colIdx, _ := col.Id()
				if pkg.IsNil(col) {
					continue
				}
				id := pkg.GenNodeId(colIdx, uint32(rowIdx))
				row := col.FindById(id)
				if row == nil {
					continue
				}
				(*defs)[i].Keys[col.Name()] = row.Value().(string)
			}
		}
	}
}
