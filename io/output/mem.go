package output

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/loanpal-engineering/exttra/pkg"
)

// "Write" an exttra tree to memory in a predefined shape
//
// 	type shape struct{
//		A string
//		B float64
// 		C time.Time
//		D struct{
//			F time.Time
//			G bool
//		}
//	}
//	outParam := make([]interface{}, 0)
//	i, err := Mem(root, shape, outParam, output.Alias("Id", "A"))
//	i.Flush()
//	for _, v := range outParam {
//		log.Printf("%v", v.(shape))
//	}
func Mem(data pkg.Composer, shape interface{}, outParam *[]interface{}, opts ...Opt) Out {
	i := new(Memory)
	i.src = data
	i.shape = shape
	if err := i.assertShape(); err != nil {
		return nil
	}
	i.out = outParam
	i.alias = make(map[uint64]reflect.StructField)
	for _, o := range opts {
		ii, er := o(i)
		if er != nil {
			log.Fatal(er)
		}
		i = ii.(*Memory)
	}
	return i
}
func (i *Memory) base() *output { return &i.output }
func (i *Memory) leftMostNode() (pkg.Composer, error) {
	// Left most node in the tree
	var lhs pkg.Composer
	// Doesnt matter which child, just pick one
	for id := range i.src.Children() {
		lhs = i.src.Children()[id]
		break
	}
	if lhs == nil {
		// todo: better error
		log.Fatal()
	}
	// Get the left most node
	for {
		if lhs.Prev() == nil {
			break
		} else {
			lhs = lhs.Prev()
		}
	}
	// move to the right until first non-null node from src nilmap
	for {
		id, _, _ := lhs.Id()
		if i.src.Null()[id] {
			if lhs.Next() != nil {
				lhs = lhs.Next()
			} else {
				// todo: better error
				log.Fatal()
			}
		} else {
			break
		}
	}
	if lhs == nil {
		return nil, errors.New("output/Memory: Failed to find left outer most node")
	}
	return lhs, nil
}
func (i *Memory) Flush() error {
	var (
		lhs     pkg.Composer
		err     error = nil
		quitter       = make(chan error)
		entity        = make(chan interface{})
		sent          = 0
	)
	if lhs, err = i.leftMostNode(); err != nil {
		return err
	}
	excludes := i.src.(pkg.Editor).Excludes()
	for _, v := range lhs.Children() {
		_, _, row := v.Id()
		if excludes[row] {
			continue
		}
		go i.fillShape(entity, quitter, v, excludes)
		sent++
	}
	if sent == 0 {
		return nil
	}
	for {
		select {
		case r := <-entity:
			sent--
			*i.out = append(*i.out, r)
			if sent == 0 {
				return nil
			}
		case err = <-quitter:
			log.Printf("output/Memory: fata error, %d rows completed before failure", sent)
			return err
		}
	}
}

// Find a field when the name is not a top level property of an object
// Use find field only for nested properties, otherwise use `elem.Type().FieldByName(name)`
func (i *Memory) findField(name string) (reflect.StructField, error) {
	var (
		path = strings.Split(name, ".")
		v    = reflect.ValueOf(i.shape)
	)
loop:
	f := v.FieldByName(path[0])
	if f.Kind() == reflect.Struct && len(path) > 1 {
		v = f
		path = path[1:]
		goto loop
	} else if len(path) == 1 {
		if pi, ok := v.Type().FieldByName(path[0]); !ok {
			return reflect.StructField{}, errors.New(fmt.Sprintf("output/Memory: can not find field %s", name))
		} else {
			return pi, nil
		}
	} else {
		return reflect.StructField{}, errors.New(fmt.Sprintf("output/Memory: failed to set shape property %s", name))
	}
}
func (i *Memory) assertShape() error {
	if reflect.ValueOf(i.shape).Kind() != reflect.Struct {
		return errors.New("output/Memory: shape must be a struct")
	}
	return nil
}

// fill the shape representing a single row, n MUST be the left most node that is part of the shape.
func (i *Memory) fillShape(out chan interface{}, quit chan error, n pkg.Composer, excludes []bool) {
	var (
		orig = reflect.ValueOf(i.shape)
		cpy  = reflect.New(orig.Type()).Elem()
	)
	for {
		var (
			field reflect.StructField
			col   = n.Parent()
		)
		cId, _, _ := col.Id()
		if i.src.Null()[cId] {
			goto next
		}

		if alias, ok := i.alias[cId]; ok {
			field = alias
		} else {
			quit <- errors.New(fmt.Sprintf("output/Memory: alias not found for column %d", cId))
		}
		if pkg.IsNil(n.Value()) {
			goto next
			// if cpy.FieldByIndex(field.Index).Type().Kind() == reflect.Ptr {
			// 	cpy.FieldByIndex(field.Index).Set(nil)
			// }
		}
		// make sure n.Value is the same type as the field
		switch n.Value().(type) {
		case string:
			if field.Type.Kind() == reflect.String {
				cpy.FieldByIndex(field.Index).SetString(n.Value().(string))
			}
		case float64:
			if field.Type.Kind() == reflect.Float64 {
				cpy.FieldByIndex(field.Index).SetFloat(n.Value().(float64))
			}
		case bool:
			if field.Type.Kind() == reflect.Bool {
				cpy.FieldByIndex(field.Index).SetBool(n.Value().(bool))
			}
		case time.Time:
			if field.Type.Kind() != reflect.Struct {
				cpy.FieldByIndex(field.Index).SetInt(n.Value().(time.Time).Unix())
			} else {
				cpy.FieldByIndex(field.Index).Set(reflect.ValueOf(n.Value()))
			}
		case int64:
			if field.Type.Kind() == reflect.Int64 {
				cpy.FieldByIndex(field.Index).SetInt(n.Value().(int64))
			}
		default:
			quit <- errors.New("output/Memory: unknown type, supported types are: string, float64, int64, bool, time.Time")
		}
	next:
		if n.Next() != nil {
			n = n.Next()
		} else {
			break
		}
	}
	out <- cpy.Interface()
}
