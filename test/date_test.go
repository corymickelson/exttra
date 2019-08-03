package test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/loanpal-engineering/exttra/data"
	"github.com/loanpal-engineering/exttra/io/input"
	"github.com/loanpal-engineering/exttra/io/output"
	"github.com/loanpal-engineering/exttra/parser"
	"github.com/loanpal-engineering/exttra/pkg"
	"github.com/loanpal-engineering/exttra/types"
	"github.com/loanpal-engineering/exttra/view"
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

		in := input.Csv(bytes.NewReader(src.Bytes()), s)
		p := parser.NewParser(&in)
		if err = p.Validate(nil); err != nil {
			t.Fatal(err)
		}
		var root pkg.Composer
		if root, err = p.Parse(); err != nil {
			t.Fatal(err)
		}
		t.Log("Select A where A is NOT NULL and A > B")
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
		type record struct {
			DateTime time.Time
		}
		var shape record
		outParam := make([]interface{}, 0)
		memOut := output.Mem(root, shape, &outParam,
			output.Alias("A", "DateTime"))
		if err = memOut.Flush(); err != nil {
			t.Fatal(err)
		} else {
			if len(test.expect) > 1 {
				if et, err := time.Parse(time.RFC3339, test.expect[1][0]); err != nil {
					t.Fatal(err)
				} else {
					if et != outParam[0].(record).DateTime {
						t.Logf("expected %v but got %v", et, outParam[0].(struct{ DateTime time.Time }).DateTime)
						t.Fail()
					}
				}
			}
		}

	}
}
