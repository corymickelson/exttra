package pkg
//
// import (
// 	"errors"
// 	"fmt"
//
// 	"github.com/loanpal-engineering/exttra/types"
// )
//
// type (
// 	NullFieldException struct {
// 		field   Composer
// 		message string
// 	}
// 	ColumnNotFoundException struct {
// 		columnIdx uint32
// 		message   string
// 	}
// 	ExpressionException struct {
// 		message string
// 		lhs     Composer
// 		rhs     Composer
// 	}
// 	ConversionException struct {
// 		message string
// 		field   Composer
// 		stage   types.Prop
// 	}
// )
//
// // Create a new NullFieldException
// // An error message can be provided with details of the null field
// func NewNullFieldException(field Composer) *NullFieldException {
// 	_, col, row := field.Id()
// 	// Get the column node
// 	parent := field.Parent()
// 	for {
// 		if IsNil(parent.Parent()) {
// 			break
// 		} else {
// 			parent = parent.Parent()
// 		}
// 	}
// 	colName := parent.Name()
// 	return &NullFieldException{
// 		field:   field,
// 		message: fmt.Sprintf("errors/NullFieldException: %s col=%d row=%d", colName, col, row),
// 	}
// }
// func (e *NullFieldException) Unwrap() error {
// 	return errors.New(e.message)
// }
// func (e *NullFieldException) Error() string {
// 	return e.message
// }
//
// // Create a new ColumnNotFoundException
// func NewColumnNotFoundException(col uint32) *ColumnNotFoundException {
// 	return &ColumnNotFoundException{
// 		columnIdx: col,
// 		message:   fmt.Sprintf("errors/ColumnNotFoundException: column %d not found", col),
// 	}
// }
// func (e *ColumnNotFoundException) Error() string {
// 	return e.message
// }
// func (e *ColumnNotFoundException) Unwrap() error {
// 	return errors.New(e.message)
// }
// func NewExpressionException(lhs, rhs Composer, msg string) *ExpressionException {
// 	_, lCol, lRow := lhs.Id()
// 	_, rCol, rRow := rhs.Id()
// 	lValue := lhs.Value()
// 	rValue := rhs.Value()
// 	return &ExpressionException{
// 		message: fmt.Sprintf("errors/ExpressionException: lhs col=%d row=%d value=%v, rhs col=%d row=%d value=%v, %s",
// 			lCol,
// 			lRow,
// 			lValue,
// 			rCol,
// 			rRow,
// 			rValue,
// 			msg),
// 		lhs: lhs,
// 		rhs: rhs,
// 	}
// }
// func (e *ExpressionException) Error() string {
// 	return e.message
// }
// func (e *ExpressionException) Unwrap() error {
// 	return errors.New(e.message)
// }
// func NewConversionException(stage types.Prop, field Composer, msg string) *ConversionException {
// 	var (
// 		stageString string
// 	)
// 	switch stage {
// 	case types.ToString:
// 		stageString = "toString"
// 	case types.Convert:
// 		stageString = "convert"
// 	case types.Extension:
// 		stageString = "extension"
// 	}
// 	_, col, row := field.Id()
// 	return &ConversionException{
// 		message: fmt.Sprintf("errors/ConversionException: field col=%d row=%d on %s failure %s",
// 			col,
// 			row,
// 			stageString,
// 			msg),
// 		field: field,
// 		stage: stage,
// 	}
// }
// func (e *ConversionException) Error() string {
// 	return e.message
// }
// func (e *ConversionException) Unwrap() error {
// 	return errors.New(e.message)
// }
