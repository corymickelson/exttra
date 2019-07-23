package pkg

type (
	Node interface {
		// Get the parent of this node. If parent is nil, this node is the root node.
		Parent() Node
		// Get the value of this node.
		Value() interface{}
		// Get the value type of this node.
		T() *FieldType
		// Get this nodes children as a map[node id]node ptr.
		Children() map[uint64]Node
		// Find a child node by either name of id.
		// If id is available use [FindById].
		// If the node is not found nil is returned.
		Find(ident interface{}) Node
		// Get a child node by id.
		// If the id is not found nil is returned.
		FindById(uint64) Node
		// Get the display name of the node. If none an empty string is returned.
		Name() string
		// Get the unique id of this cell.
		// Id is composed of two 32 bit integers.
		// Id will return the 64bit container, the two 32 bit unsigned
		// integers (col, row), and an error if applicable
		Id() (uint64, uint32, uint32)
		// Add a new node to this nodes child collection.
		// Set [b] = true if this node should be hidden from in the output
		Add(Node, bool) error
		Next(set ...Node) Node
		Prev(set ...Node) Node
		Min() uint64
		Max() uint64
		Nulls() map[uint64]bool
		// Set the version back to 0 for this node and all it's children
		Reset()
	}
	NodeModifier interface {
		Node
		// Create a new version.
		// Increment version number, and create a new nodemap/nilmap
		// a reference of this nilmap is returned to the caller so they may update as needed
		Version() *map[uint64]bool
		Toggle(uint64)
		// Build an aggregated view of all excluded rows
		// Array index is the row number; true should be excluded and false kept
		Excludes() []bool
		Excluded(id uint64) (bool, error)
	}
)

func GenNodeId(col uint32, row uint32) uint64 {
	return uint64(col)<<32 | uint64(row)
}
