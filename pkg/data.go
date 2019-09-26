package pkg

type (
	Nullable struct {
		Variants    []string // 24
		ReplaceWith *string  // 8
		Allowed     bool     // 1
	}
	// Composer is the primary building block of the exttra tree
	Composer interface {
		// Get the parent of this node. If parent is nil, this node is the root node.
		Parent() Composer
		// Get the value of this node.
		Value() interface{}
		// Get the value type of this node.
		T() FieldType
		// Get this nodes children as a map[node id]node ptr.
		Children() *map[uint64]Composer
		// Find a child node by either name of id.
		// If id is available use [FindById].
		// If the node is not found nil is returned.
		Find(ident interface{}) Composer
		// Get a child node by id.
		// If the id is not found nil is returned.
		FindById(uint64) Composer
		// Get the display name of the node. If none an empty string is returned.
		Name() string
		// Get the unique id of this cell.
		// Id is composed of two 32 bit integers.
		// Id will return the 64bit container, with two 32 bit unsigned
		// integers (col, row).
		// It is possible to get two nodes with the same Id, this is only true
		// for the root node, and the left most node at child[0]. Due to this circumstance,
		// if using the Id for finding a nodes placement, one should first check that
		// the current node is not the root (a root's parent is nil)
		Id() (uint64, uint32, uint32)
		// Add a new node to this nodes child collection.
		// Set [b] = true if this node should be hidden from in the output
		Add(Composer, bool) error
		// Get/Set the Next node in this row
		Next(set ...Composer) Composer
		// Get/Set the Prev node in this row
		Prev(set ...Composer) Composer
		// The min row index of children. This value is created based off the Id of a child when it's [Add]ed to the node
		Min() uint64
		// The max row index of children. This value is created based off the Id of a child when it's [Add]ed to the node
		Max() uint64
		// Get the nilmap of a node, the nilmap is an aggregate of all children, nilmaps are used for setting the
		// visibility of a child node. Visibility can be determined by the schema, parser, expression evaluation, or
		// manually via explicit manipulating a node nilmap (not advised).
		Null() map[uint64]bool
		// Get the nullability of a node
		Nullable() Nullable // Reset()
		// Is the child node at [id] excluded (not visible at output)
		Excluded(id uint64) (bool, error)
		// Find a child node by value. This method only works if the node was constructed with the functional option Index()
		// GetIndexed returns an array of id's as indexed nodes are not distinct.
		GetIndexed(interface{}) ([]uint64, error)
	}
	// An Editor interface operates from the root node down. Any operation performed with an Editor entity will effect
	// the entire tree. These effects are however complimentary to the value of the tree. Once a tree is constructed ( during parsing )
	// the tree's value is set and can not be modified. Editor methods operate only on the visibility of the tree; allowing
	// something to be seen by the output method based on an expression or direct manipulation of a nodes nilmap
	Editor interface {
		// Fork creates a new clean nilmap of the entire tree.
		// The original tree is immutable, after it's created it can not be changed, however the view of the
		// tree can. To create new views a new fork is needed. Fork(ing) the tree provides a clean mapping of what
		// the output can and can not see.
		Fork() *map[uint64]bool
		// Toggle the visibility of a child node. A nodes children's visibility is implicitly set during parsing, and expression evaluation
		// (views) but may also by set through the [Toggle] method.
		// Setting an Id to true will hide a node from the [Out] instance; telling the [Out] instance that this child is nil.
		// Setting it to false will allow the node to be seen by the [Out] instance; telling the [Out] instance this child has a value to be viewed.
		// If this seem unintuitive or backwards, recall this method is manipulating a nodes NILmap; a mapping of all child
		// nodes which are nil (non-viewable) to an [Out] instance
		Toggle(uint64, bool)
		// Build an aggregated view of all excluded rows
		Excludes() []bool
		// Reset the tree to the initial visibility construction. To see how visibility is created during construction
		// see [Parser]
		Reset()
		// LockWhile creates a read lock on the root node while the function is ran
		// LockWhile(func())
	}
	Defector interface {
		Report(originalOffset int) [][]string // in csv format
		Coll() *[]*Defect
		Count() int
	}
	Defects struct {
		exitInterrupt func([]*Defect)
		Headers       []string
		coll          []*Defect
		enabled       bool
	}
	Opt    func(*Defects) (*Defects, error)
	Defect struct {
		Row  int
		Col  int
		Keys map[string]string
		Msg  string
	}
)

func GenNodeId(col uint32, row uint32) uint64 {
	return uint64(col)<<32 | uint64(row)
}
