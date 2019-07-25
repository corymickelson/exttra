package test

import (
	"bytes"
	"encoding/csv"
	"strings"
	"testing"

	"github.com/corymickelson/exttra/data"
	"github.com/corymickelson/exttra/io/input"
	"github.com/corymickelson/exttra/io/output"
	"github.com/corymickelson/exttra/parser"
	"github.com/corymickelson/exttra/pkg"
	"github.com/corymickelson/exttra/types"
	"github.com/corymickelson/exttra/view"
)

func generateFile(f [][]string) *bytes.Buffer {
	var b strings.Builder
	for i, r := range f {
		row := strings.Join(r, ",")
		b.WriteString(row)
		if i < len(f)-1 {
			b.WriteString("\n")
		}
	}
	return bytes.NewBufferString(b.String())
}
func TestDate(t *testing.T) {
	testDate(t)
}
func BenchmarkDate(b *testing.B) {
	testDate(b)
}
func testDate(i interface{}) {
	t, ok := i.(testing.TB)
	if !ok {
		return
	}
	var (
		fail = func(it interface{}) {
			if it != nil {
				t.Fatal(it)
			}
		}
		err          error        = nil
		null         pkg.Composer = nil
		explicitNull              = "NULL"
	)
	nullable := &pkg.Nullable{Allowed: true, Variants: []string{"null", "NULL", ""}, ReplaceWith: &explicitNull}
	nullableDate, err := types.NewField(&types.Field{T: pkg.DATE}, nullable)
	nonNil := &pkg.Nullable{Allowed: false}
	nonNilDate, err := types.NewField(&types.Field{T: pkg.DATE}, nonNil)
	if null, err = data.NewNode(nil, data.V(nil)); err != nil {
		t.Fatal()
	}
	table := []struct {
		file   [][]string
		fields []*types.Field
		expect [][]string
	}{
		{
			file: [][]string{
				{"A", "B", "C", "D"},
				{"12/01/2018", "01/10/1920", "01/03/1999", "12/1/2018"},
			},
			fields: []*types.Field{
				nonNilDate, nonNilDate, nonNilDate, nonNilDate,
			},
			expect: [][]string{
				{"A"},
				{"2018-12-01T00:00:00Z"},
			},
		},
		{
			file: [][]string{
				{"A", "B", "C", "D"},
				{"", "1/10/2020", "01/03/1999", "5/5/2000"},
			},
			fields: []*types.Field{
				nullableDate, nonNilDate, nonNilDate, nonNilDate,
			},
			expect: [][]string{
				{"A"},
			},
		},
		{
			file: [][]string{
				{"A", "B", "C", "D"},
				{"foo bar", "01/03/1999", "1/10/2020", "5/5/2000"},
			},
			fields: []*types.Field{
				nullableDate, nonNilDate, nonNilDate, nonNilDate,
			},
			expect: [][]string{
				{"A"},
			},
		},
	}

	for _, test := range table {
		src := generateFile(test.file)
		s := types.NewSchema(
			types.Column("A", test.fields[0], true),
			types.Column("B", test.fields[1], true),
			types.Column("C", test.fields[2], true),
			types.Column("D", test.fields[3], true),
		)

		in := input.CsvIn(bytes.NewReader(src.Bytes()), s)
		p := parser.NewParser(&in)
		if err = p.Validate(nil); err != nil {
			t.Fatal(err)
		}
		var root pkg.Composer
		if root, err = p.Parse(); err != nil {
			t.Fatal(err)
		}
		t.Log("Validate and Parse OK")
		err = view.NewView(view.Select("A"),
			view.From(root),
			view.Where(
				pkg.And{
					pkg.Not{pkg.Eq{root.Find("A"), null}},
					pkg.Gt{root.Find("A"), root.Find("B")},
				},
			))
		if err != nil {
			t.Fatal(err)
		}
		mem := new(bytes.Buffer)
		out := output.CsvOut(root, mem)
		if err = out.Flush(); err != nil {
			t.Fatal(err)
		}
		tr := csv.NewReader(bytes.NewReader(mem.Bytes()))
		actual, err := tr.ReadAll()
		fail(err)
		for i, v := range actual {
			for ii, vv := range v {
				if actual[i][ii] != vv {
					t.Fail()
				}
			}
		}
	}
}
