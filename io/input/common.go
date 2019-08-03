package input

import (
	"encoding/csv"
	"github.com/loanpal-engineering/exttra/types"
	"io"
)

type (
	Input interface {
		GetSchema() types.Signature
		GetReader() interface{}
	}
	flatFile struct {
		source     io.Reader
		definition *types.Schema
		reader     *csv.Reader
	}
)
