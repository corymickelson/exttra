package pkg

type (
	FieldType           int
	FieldLevelConverter func(*string, ...interface{}) (interface{}, error)
	FieldExtension      func(it interface{}) (interface{}, error)
	StringifyField      func(it interface{}) *string
	Pair                struct {
		First, Second interface{}
	}
)

const (
	// INT FieldType = iota
	INT8 FieldType = iota
	INT16
	INT
	INT32
	INT64
	UINT8
	UINT16
	UINT
	UINT32
	UINT64
	FLOAT32
	FLOAT
	FLOAT64
	STRING
	TIMESTAMP
	DATE
	CUSTOM
	BOOL
	NULL
	UNKNOWN
)

func (dt FieldType) String() string {
	return [...]string{
		"INT8",
		"INT16",
		"INT",
		"INT32",
		"INT64",
		"UINT8",
		"UINT16",
		"UINT",
		"UINT32",
		"UINT64",
		"FLOAT32",
		"FLOAT",
		"FLOAT64",
		"STRING",
		"TIMESTAMP",
		"DATE",
		"CUSTOM",
		"BOOL",
		"NULL",
		"UNKNOWN",
	}[dt]
}
