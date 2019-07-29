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

// Create a new pkg.Composer.
// Id [id] must be unique, this is is used for views, and writing to an output format.
// Ids are an unsigned 64 bit integer where the first 32 bits are the column index
// and the later 32 bits the row index.
// Optional properties [opts] can also be passed during construction:
// 		Type: the value type held by this node
// 		V: the value
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
func (i *node) Parent() pkg.Composer {
	return i.parent
}
func (i *node) Value() interface{} {
	return i.v
}
func (i *node) Children() map[uint64]pkg.Composer {
	return i.children
}
func (i *node) Name() string {
	return i.name
}
func (i *node) Add(n pkg.Composer, b bool) error {
	i.nm[i.version][n.(*node).id] = b
	i.children[n.(*node).id] = n
	n.(*node).parent = i
	if i.max < n.(*node).id&0xFFFFFFFF {
		i.max = n.(*node).id & 0xFFFFFFFF
	}
	return nil
}
func (i *node) Min() uint64 {
	return i.min
}
func (i *node) Max() uint64 {
	return i.max
}
func (i *node) Null() map[uint64]bool {
	return i.nm[i.version]
}
func (i *node) reset() {
	i.version = 0
	for _, v := range i.children {
		v.(*node).reset()
	}
}
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
func (i *node) Fork() *map[uint64]bool {
	n := root(i)
	n.mutex.Lock()
	defer n.mutex.Unlock()
	n.fork()
	return &n.nm[n.version]
}
func (i *node) Id() (uint64, uint32, uint32) {
	col := uint32(i.id >> 32)
	row := uint32(i.id & 0xFFFFFFFF)
	return i.id, col, row
}
func (i *node) T() *pkg.FieldType {
	return i.t
}
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
func (i *node) FindById(id uint64) pkg.Composer {
	if c, ok := i.children[id]; ok {
		return c
	} else {
		return nil
	}
}
func (i *node) Next(set ...pkg.Composer) pkg.Composer {
	if len(set) > 0 {
		if pkg.IsNil(i.next) {
			i.next = set[0]
		}
	}
	return i.next
}
func (i *node) Prev(set ...pkg.Composer) pkg.Composer {
	if len(set) > 0 {
		if pkg.IsNil(i.prev) {
			i.prev = set[0]
		}
	}
	return i.prev
}
func (i *node) Toggle(id uint64, b bool) {
	i.nm[i.version][id] = b
}
func (i *node) Excluded(id uint64) (bool, error) {
	if ex, ok := i.nm[i.version][id]; !ok {
		return false, errors.New(fmt.Sprintf("data/tree: id %d does not exist in node", id))
	} else {
		return ex, nil
	}
}
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
