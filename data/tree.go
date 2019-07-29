package data

import (
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/corymickelson/exttra/pkg"
)

type (
	node struct {
		id       uint64
		name     string
		v        interface{}
		parent   pkg.Composer
		t        *pkg.FieldType
		children map[uint64]pkg.Composer
		nm       []map[uint64]bool
		nullable *pkg.Nullable
		version  uint
		next     pkg.Composer
		prev     pkg.Composer
		min      uint64
		max      uint64
		mutex    sync.RWMutex
	}

	Opt func(*node) (*node, error)
)

// Create a new node, returned as a pkg.Composer.
// Id [id] must be unique, this is is used for views, and writing to an output format.
// Ids are an unsigned 64 bit integer where the first 32 bits are the column index
// and the later 32 bits the row index.
//
// 	null := &pkg.Nullable({Allowed: false})
// 	root := NewNode(1, V(""), Name("Test"), Type(pkg.STRING), Nullable(null))
//
// The code above has created a new node with an id of 1, an empty string as the value, a name of "Test"
// a type of STRING, and a nullable option explicitly setting this node and it's children where nil will throw
//
// The creation of nodes are largely handled for the user via parser, and view.
// However when creating views where the comparator is fixed, a new node with the fixed value is required.
func NewNode(id *uint64, opts ...Opt) (pkg.Composer, error) {
	var err error = nil
	i := new(node)
	if id != nil {
		i.id = *id
	}
	i.parent = nil
	i.nm = make([]map[uint64]bool, 1)
	i.nm[0] = make(map[uint64]bool)
	i.children = make(map[uint64]pkg.Composer)
	for _, op := range opts {
		i, err = op(i)
		if err != nil {
			log.Fatalf("data/tree: option error %s", err.Error())
		}
	}
	return i, nil
}

// Set the nullable property of a node
func Nullable(nullable *pkg.Nullable) Opt {
	return func(n *node) (*node, error) {
		n.nullable = nullable
		return n, nil
	}
}

// Add the type [t] of value held by this node.
func Type(t *pkg.FieldType) Opt {
	return func(n *node) (*node, error) {
		n.t = t
		return n, nil
	}
}

// Add the value [v] to this node.
func V(v interface{}) Opt {
	return func(n *node) (*node, error) {
		n.v = v
		return n, nil
	}
}

// Add the display name to the node. This property can be used
// as a column header when the output is csv.
func Name(name string) Opt {
	return func(n *node) (*node, error) {
		n.name = name
		return n, nil
	}
}

// Get the parent node or nil
// The root node will NOT have a parent
func (i *node) Parent() pkg.Composer {
	return i.parent
}

// Get the value of the node. Values are the converted (native go type) value.
// For example if this column is defined as a FLOAT and the cell in the csv has the value "10.00"
// The Value of this node would be a go float64 10.00
func (i *node) Value() interface{} {
	return i.v
}

// Get a nodes children
func (i *node) Children() map[uint64]pkg.Composer {
	return i.children
}

// Get a nodes name
// Names must be explicitly set during instantiation of the new node
func (i *node) Name() string {
	return i.name
}

// Add a child node
// Children are add as the node, nil bit
// If a child has a nil value or should be invisible to the output package the nilbit must be false
func (i *node) Add(n pkg.Composer, b bool) error {
	i.nm[i.version][n.(*node).id] = b
	i.children[n.(*node).id] = n
	n.(*node).parent = i
	if i.max < n.(*node).id&0xFFFFFFFF {
		i.max = n.(*node).id & 0xFFFFFFFF
	}
	return nil
}

// Min row contained in this nodes child collection
func (i *node) Min() uint64 {
	return i.min
}

// Max row contained in this nodes child collection
func (i *node) Max() uint64 {
	return i.max
}

// Get this nodes nilmap
func (i *node) Null() map[uint64]bool {
	return i.nm[i.version]
}
func (i *node) reset() {
	i.version = 0
	for _, v := range i.children {
		v.(*node).reset()
	}
}

// Reset the entire tree back to it's initial state (after parsing).
// This method locks the entire tree until all nodes have been reset
func (i *node) Reset() {
	n := root(i)
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.reset()
}
func root(i *node) *node {
	for {
		if i.parent == nil {
			return i
		} else {
			i = i.parent.(*node)
		}
	}
}
func (i *node) fork() {
	v := make(map[uint64]bool)
	for id := range i.nm[0] {
		// default nilmap to true, ie all nodes are nil
		v[id] = true
	}
	// v = i.nm[0]
	i.nm = append(i.nm, v)
	i.version = uint(len(i.nm)) - 1
	if len(i.children) > 0 {
		for _, v := range i.children {
			v.(*node).fork()
		}
	}
}

// Create a new version of the tree.
// New versions are merely adding a new nilmap to the node and all it's children.
// A reference to the new nilmap is returned to the caller.
// At this point the caller can now toggle nodes; making them visible or invisible to
// the output object.
func (i *node) Fork() *map[uint64]bool {
	n := root(i)
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.fork()
	return &n.nm[n.version]
}

// Get a nodes Id.
// Return value is the Id as well as the col and row contained in the Id
func (i *node) Id() (uint64, uint32, uint32) {
	col := uint32(i.id >> 32)
	row := uint32(i.id & 0xFFFFFFFF)
	return i.id, col, row
}

// Get the Type of this node
func (i *node) T() *pkg.FieldType {
	return i.t
}

// Find a child node by either Name, or Id
// If Id is available, use [FindById]
func (i *node) Find(ident interface{}) pkg.Composer {
	var (
		name  *string
		index *uint64
		ii    uint64
		c     pkg.Composer
		ok    = false
	)

	switch ident.(type) {
	case string:
		n := ident.(string)
		name = &n
	case uint64:
		ui := ident.(uint64)
		index = &ui
	}
	if index != nil {
		if c, ok = i.children[*index]; ok {
			return c
		}
	}
	for ii, c = range i.children {
		if index != nil && ii == *index {
			return c
		}
		if name != nil && *name == c.(*node).name {
			return c
		}
	}
	return nil
}

// Find a node by Id
func (i *node) FindById(id uint64) pkg.Composer {
	if c, ok := i.children[id]; ok {
		return c
	} else {
		return nil
	}
}

// Get the next node in this row
func (i *node) Next(set ...pkg.Composer) pkg.Composer {
	if len(set) > 0 {
		if pkg.IsNil(i.next) {
			i.next = set[0]
		}
	}
	return i.next
}

// Get the previous node in this row
func (i *node) Prev(set ...pkg.Composer) pkg.Composer {
	if len(set) > 0 {
		if pkg.IsNil(i.prev) {
			i.prev = set[0]
		}
	}
	return i.prev
}

// Toggle the visibility of a child node where [id] is the childs id and [b] is the value of this node in the nilmap
func (i *node) Toggle(id uint64, b bool) {
	i.nm[i.version][id] = b
}

// Is [id] excluded.
// Exclusions are determined by the nilmap
func (i *node) Excluded(id uint64) (bool, error) {
	if ex, ok := i.nm[i.version][id]; !ok {
		return false, errors.New(fmt.Sprintf("data/tree: id %d does not exist in node", id))
	} else {
		return ex, nil
	}
}

// Merge all nilmaps into one
func (i *node) Excludes() []bool {
	var (
		root            = i
		excludes []bool = nil
	)
	// Get the root node.
	for {
		if pkg.IsNil(root.parent) {
			break
		} else {
			root = root.parent.(*node)
		}
	}
	for id, col := range root.children {
		if root.Null()[id] {
			continue
		}
		if excludes == nil {
			excludes = make([]bool, col.(*node).max+1)
		}
		if col.(*node).nullable.Allowed {
			continue
		}
		for _, vv := range col.Children() {
			id, _, row := vv.Id()
			if ex, _ := col.(*node).Excluded(id); ex {
				excludes[row] = true
			}
		}
	}
	return excludes
}
func (i *node) Nullable() *pkg.Nullable {
	return i.nullable
}
func (i *node) LockWhile(fn func()) {
	n := root(i)
	n.mutex.RLock()
	defer n.mutex.RUnlock()
	fn()
}
