package pkg

type (
	FieldType           int
	FieldLevelConverter func(*string) (interface{}, error)
	FieldExtension      func(it interface{}) (interface{}, error)
	StringifyField      func(it interface{}) *string
	Pair                struct {
		First, Second interface{}
	}
)

const (
	INT FieldType = iota
	FLOAT
	STRING
	TIMESTAMP
	DATE
	CUSTOM
	BOOL
	NULL
	UNKNOWN
)

func (dt FieldType) String() string {
	return [...]string{"INT", "FLOAT", "STRING", "TIMESTAMP", "DATE", "CUSTOM", "BOOL", "NULL", "UNKNOWN"}[dt]
}
