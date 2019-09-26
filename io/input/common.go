package input

import (
	"encoding/csv"
	"io"

	"github.com/loanpal-engineering/exttra/types"
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
