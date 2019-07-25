package input

import (
	"encoding/csv"
	"io"
	"log"

	"github.com/corymickelson/exttra/types"
)

type Input interface {
	GetSchema() types.Signature
	GetReader() interface{}
}
type CsvInput struct {
	source     io.Reader
	definition *types.Schema
	reader     *csv.Reader
}

// Get the schema of this input
func (i *CsvInput) GetSchema() types.Signature {
	return i.definition
}

// Get a reader. Reader will provide a read and/or readAll method to consume the file
func (i *CsvInput) GetReader() interface{} {
	return i.reader
}

// Create a new input object.
func CsvIn(source io.Reader, def types.Signature) Input {
	if def == nil {
		log.Fatal("input.CsvIn schema is required")
	}
	original, ok := def.(*types.Schema)
	if !ok {
		// todo: Signature is an empty interface, used only to mask the full schema object, this cast can
		// break, how to handle casting error
		log.Fatal("input.CsvIn schema bad cast")
	}
	i := new(CsvInput)
	i.source = source
	i.definition = original
	i.reader = csv.NewReader(i.source)
	i.reader.TrimLeadingSpace = true
	i.reader.ReuseRecord = true
	return i
}
