package view

import (
	"fmt"

	"github.com/loanpal-engineering/exttra/pkg"
	"github.com/pkg/errors"
)

type (
	Opt  func(*view, uint32) (*view, error)
	view struct {
		root         pkg.Composer
		selectClause []string
		keymap       map[uint32]interface{}
	}
)

func From(node pkg.Composer) Opt {
	return func(v *view, idx uint32) (*view, error) {
		if v.root != nil {
			return nil, errors.New("view/builder: root node has already been set")
		}
		v.root = node
		return v, nil
	}
}
func Select(fields ...string) Opt {
	return func(v *view, idx uint32) (*view, error) {
		v.selectClause = fields
		return v, nil
	}
}

// A bastardized where clause.
// Use pkg/ops to compose an expression in which resulting columns that evaluate to [true]
// are passed to the output to be viewed, and where [false] are hidden from output, thus not viewable.
func Where(clause pkg.Operator) Opt {
	return func(v *view, idx uint32) (*view, error) {
		m, t := clause.Apply()
		if t != pkg.BOOL {
			return v, errors.New("where clause expressions must evaluate to boolean(s)")
		}
		v.keymap = m
		return v, nil
	}
}

// Create a new view
// Views do not mutate the underlying data.
// Results from the view expression [Where] are reflected only in a nodes nm (Composer Map), and version number.
// To revert back to the last version of a node, or of the entire tree, call [Reset] to point each node back to it's
// original mapping.
// NewView will add a new version and mapping to the node defined in the [From] option.
// A new node is NOT returned. To access the results of a view use the same node that was provided to [From]
// Again, to revert back to the original tree, call root.Reset()
func NewView(opts ...Opt) error {
	var err error = nil
	i := new(view)
	i.root = nil
	for ii, op := range opts {
		i, err = op(i, uint32(ii))
		if err != nil {
			return err
		}
	}
	if pkg.IsNil(i.root) {
		return errors.New("view/builder: view [From] must be defined with the [From] view option")
	}
	if len(i.selectClause) == 0 {
		return errors.New("view/builder: can not create a view without one or more selected nodes")
	}
	vNext := i.root.(pkg.Editor).Fork()
	for _, name := range i.selectClause {
		col := i.root.Find(name)
		if pkg.IsNil(col) {
			return errors.New(fmt.Sprintf("view/builder: selected field %s could not be found in table %v", name, i.root))
		}
		id, colIdx, _ := col.Id()
		(*vNext)[id] = false // toggle visible
		for rowIdx, v := range i.keymap {
			// iid := uint64(colIdx)<<32 | uint64(rowIdx)
			iid := pkg.GenNodeId(colIdx, rowIdx)
			col.(pkg.Editor).Toggle(iid, !v.(bool))
		}
	}
	return nil
}
