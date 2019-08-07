package pkg

import (
	"log"
	"time"

	"github.com/pkg/errors"
)

type (
	// All operators must follow the convention:
	// Any fixed or null values must be on the right hand side
	Operator interface {
		// applies the two nodes with an expression.
		// this results in a map with the row as the index and the expression result as the value
		Apply() (map[uint32]interface{}, FieldType)
	}

	Lt struct {
		Lhs, Rhs Composer
	}

	Gt struct {
		Lhs, Rhs Composer
	}

	Eq struct {
		Lhs, Rhs Composer
	}

	Addition struct {
		Lhs, Rhs Composer
	}

	Subtraction struct {
		Lhs, Rhs Composer
	}

	Not struct {
		Value Operator
	}

	And struct {
		Lhs, Rhs Operator
	}
	Or struct {
		Lhs, Rhs Operator
	}
	If struct {
		Cond Operator
		Then Operator
		Else Operator
	}
	True  struct{}
	False struct{}
)

func assertTypeIn(ts []FieldType, l, r Composer) (*FieldType, error) {
	lOk := false
	rOk := false
	var lt FieldType
	var rt FieldType
	for _, tt := range ts {
		if rOk && lOk {
			break
		}
		if l.T() != nil && *l.T() == tt {
			lt = tt
			lOk = true
		}
		if r.T() != nil && *r.T() == tt {
			rt = tt
			rOk = true
		}
	}
	if !lOk || !rOk {
		return nil, errors.New("pkg/ops:  left and right could not be casted to a pkg.FieldType")
	}
	if rt != lt {
		return nil, errors.New("pkg/ops: types do not match")
	}
	return &lt, nil
}

// Applies the function to each left and right node pair. The right hand side may be a single fixed value,
// in which case every value on the left is compared to a single right hand side value.
// Results are gathered in a map where the key is the row index and the value is the result of the function
func applyToT(t interface{}, out chan map[uint32]interface{}, op func(l, r interface{}) interface{}, l, r Composer) {
	var (
		results = make(map[uint32]interface{})
		fixed   = r.Max() == 0
	)
	excludes := l.(Editor).Excludes()
	for _, ll := range l.Children() {
		var (
			lv = ll.Value()
			rv interface{}
		)
		id, _, row := ll.Id()
		if excludes[row] || l.Null()[id] {
			results[row] = false
			continue
		}
		lv = ll.Value()
		if fixed {
			rv = r.Value()
		} else {
			rhsId, rcol, rrow := r.Id()
			if excludes[rrow] || r.Null()[rhsId] {
				results[row] = false
				continue
			}
			rv = r.FindById(GenNodeId(rcol, row)).Value()
		}

		switch t.(type) {
		case time.Time:
			if _, ok := lv.(time.Time); !ok {
				log.Printf("can not cast \"%v\" to time.Time", lv)
				results[row] = nil
			} else if _, ok := rv.(time.Time); !ok {
				log.Printf("can not cast \"%v\" to time.Time", rv)
				results[row] = nil
			} else {
				results[row] = op(lv, rv)
			}
		case float64:
			if _, ok := lv.(float64); !ok {
				log.Printf("can not cast \"%v\" to float", lv)
				results[row] = nil
			} else if _, ok := rv.(float64); !ok {
				log.Printf("can not cast \"%v\" to float", rv)
				results[row] = nil
			} else {
				results[row] = op(lv, rv)
			}
		case string:
			if _, ok := lv.(string); !ok {
				log.Printf("can not cast \"%v\" to string", lv)
				results[row] = nil
			} else if _, ok := rv.(string); !ok {
				log.Printf("can not cast \"%v\" to string", rv)
				results[row] = nil
			} else {
				results[row] = op(lv, rv)
			}
		case bool:
			if _, ok := lv.(bool); !ok {
				log.Printf("can not cast \"%v\" to bool", lv)
				results[row] = nil
			} else if _, ok := rv.(bool); !ok {
				log.Printf("can not cast \"%v\" to bool", rv)
				results[row] = nil
			} else {
				results[row] = op(lv, rv)
			}
		default:
			// todo: handle error
		}
	}
	out <- results
}
func (lt Lt) Apply() (map[uint32]interface{}, FieldType) {
	var (
		t   *FieldType
		err error
	)
	if t, err = assertTypeIn(
		[]FieldType{INT, TIMESTAMP, FLOAT, DATE, STRING}, lt.Lhs, lt.Rhs); err != nil {
		log.Fatal(err)
	}
	out := make(chan map[uint32]interface{})
	var outType FieldType
	switch *t {
	case STRING:
		go applyToT("", out, func(lv, rv interface{}) interface{} { return lv.(string) < rv.(string) }, lt.Lhs, lt.Rhs)
		outType = BOOL
	case INT:
		fallthrough
	case FLOAT:
		go applyToT(float64(0), out, func(l, r interface{}) interface{} { return l.(float64) < r.(float64) }, lt.Lhs, lt.Rhs)
		outType = BOOL
	case DATE:
		fallthrough
	case TIMESTAMP:
		go applyToT(time.Time{}, out, func(lv, rv interface{}) interface{} { return lv.(time.Time).Unix() < rv.(time.Time).Unix() }, lt.Lhs, lt.Rhs)
		outType = BOOL
	default:
		log.Fatal("can not apply unknown in expression")
	}
	v := <-out
	return v, outType
}
func (gt Gt) Apply() (map[uint32]interface{}, FieldType) {
	var (
		t   *FieldType
		err error
	)
	if t, err = assertTypeIn(
		[]FieldType{INT, TIMESTAMP, FLOAT, DATE, STRING}, gt.Lhs, gt.Rhs); err != nil {
		log.Fatal(err)
	}
	out := make(chan map[uint32]interface{})
	var outType FieldType
	switch *t {
	case STRING:
		go applyToT("", out, func(lv, rv interface{}) interface{} { return lv.(string) > rv.(string) }, gt.Lhs, gt.Rhs)
		outType = BOOL
	case INT:
		fallthrough
	case FLOAT:
		go applyToT(float64(0), out, func(l, r interface{}) interface{} { return l.(float64) > r.(float64) }, gt.Lhs, gt.Rhs)
		outType = BOOL
	case DATE:
		fallthrough
	case TIMESTAMP:
		go applyToT(time.Time{}, out, func(lv, rv interface{}) interface{} { return lv.(time.Time).Unix() > rv.(time.Time).Unix() }, gt.Lhs, gt.Rhs)
		outType = BOOL
	default:
		log.Fatal("can not apply unknown in expression")
	}
	v := <-out
	return v, outType
}
func (eq Eq) Apply() (map[uint32]interface{}, FieldType) {
	var (
		t   *FieldType
		err error
	)
	if eq.Rhs.Value() == nil {
		nm := make(map[uint32]interface{})
		for i, v := range eq.Lhs.Null() {
			row := uint32(i & 0xFFFFFFFF)
			nm[row] = v
		}
		return nm, BOOL
	}
	if t, err = assertTypeIn(
		[]FieldType{INT, TIMESTAMP, FLOAT, DATE, STRING, BOOL}, eq.Lhs, eq.Rhs); err != nil {
		log.Fatal(err)
	}
	out := make(chan map[uint32]interface{})
	var outType FieldType
	switch *t {
	case STRING:
		go applyToT("", out, func(lv, rv interface{}) interface{} { return lv.(string) == rv.(string) }, eq.Lhs, eq.Rhs)
		outType = BOOL
	case INT:
		fallthrough
	case FLOAT:
		go applyToT(float64(0), out, func(l, r interface{}) interface{} { return l.(float64) == r.(float64) }, eq.Lhs, eq.Rhs)
		outType = BOOL
	case DATE:
		fallthrough
	case TIMESTAMP:
		go applyToT(time.Time{}, out, func(lv, rv interface{}) interface{} { return lv.(time.Time).Unix() == rv.(time.Time).Unix() }, eq.Lhs, eq.Rhs)
		outType = BOOL
	case BOOL:
		go applyToT(true, out, func(l, r interface{}) interface{} {
			return l.(bool) == r.(bool)
		}, eq.Lhs, eq.Rhs)
		outType = BOOL
	default:
		log.Fatal("can not apply unknown in expression")
	}
	v := <-out
	return v, outType
}
func (a Addition) Apply() (map[uint32]interface{}, FieldType) {
	var (
		t   *FieldType
		err error = nil
	)
	if t, err = assertTypeIn(
		[]FieldType{INT, TIMESTAMP, FLOAT, DATE, STRING}, a.Lhs, a.Rhs); err != nil {
		log.Fatal(err)
	}
	out := make(chan map[uint32]interface{})
	var outType FieldType
	switch *t {
	case STRING:
		go applyToT("", out, func(lv, rv interface{}) interface{} { return lv.(string) + rv.(string) }, a.Lhs, a.Rhs)
		outType = STRING
	case INT:
		fallthrough
	case FLOAT:
		go applyToT(float64(0), out, func(l, r interface{}) interface{} { return l.(float64) + r.(float64) }, a.Lhs, a.Rhs)
		outType = FLOAT
	case DATE:
		fallthrough
	case TIMESTAMP:
		go applyToT(time.Time{}, out, func(lv, rv interface{}) interface{} {
			return time.Unix(lv.(time.Time).Unix()+rv.(time.Time).Unix(), 0)
		}, a.Lhs, a.Rhs)
		outType = TIMESTAMP
	default:
		log.Fatal("can not apply unknown in expression")
	}
	v := <-out
	return v, outType
}
func (sub Subtraction) Apply() (map[uint32]interface{}, FieldType) {
	var (
		t   *FieldType
		err error
	)
	if t, err = assertTypeIn(
		[]FieldType{INT, TIMESTAMP, FLOAT, DATE}, sub.Lhs, sub.Rhs); err != nil {
		log.Fatal(err)
	}
	out := make(chan map[uint32]interface{})
	var outType FieldType
	switch *t {
	case INT:
		fallthrough
	case FLOAT:
		go applyToT(float64(0), out, func(l, r interface{}) interface{} { return l.(float64) - r.(float64) }, sub.Lhs, sub.Rhs)
		outType = FLOAT
	case DATE:
		fallthrough
	case TIMESTAMP:
		go applyToT(time.Time{}, out, func(lv, rv interface{}) interface{} {
			return time.Unix(lv.(time.Time).Unix()-rv.(time.Time).Unix(), 0)
		}, sub.Lhs, sub.Rhs)
		outType = TIMESTAMP
	default:
		log.Fatal("can not apply unknown in expression")
	}
	v := <-out
	return v, outType
}
func (not Not) Apply() (map[uint32]interface{}, FieldType) {
	nm, t := not.Value.Apply()
	out := make(map[uint32]interface{})
	for i, v := range nm {
		out[i] = !v.(bool)
	}
	return out, t
}
func (t True) Apply() (map[uint32]interface{}, FieldType) {
	return map[uint32]interface{}{1: true}, BOOL
}
func (f False) Apply() (map[uint32]interface{}, FieldType) {
	return map[uint32]interface{}{1: false}, BOOL
}
func (c If) Apply() (map[uint32]interface{}, FieldType) {
	var (
		async = func(prop string, out chan Pair, op Operator) {
			m, t := op.Apply()
			if t != BOOL {
				log.Fatal("Logical statements must evaluate to a boolean")
			}
			out <- Pair{prop, m}
		}
		coll  = make(chan Pair)
		cond  map[uint32]interface{}
		t     map[uint32]interface{}
		e     map[uint32]interface{}
		final = make(map[uint32]interface{})
	)
	go async("cond", coll, c.Cond)
	go async("then", coll, c.Then)
	go async("else", coll, c.Else)
	for {
		select {
		case m := <-coll:
			if m.First == "cond" {
				cond = m.Second.(map[uint32]interface{})
			} else if m.First == "then" {
				t = m.Second.(map[uint32]interface{})
			} else {
				e = m.Second.(map[uint32]interface{})
			}
			if cond != nil && t != nil && e != nil {
				var (
					fixedThen      = false
					fixedThenValue bool
					fixedElse      = false
					fixedElseValue bool
				)
				switch c.Then.(type) {
				case True:
					fixedThen = true
					fixedThenValue = true
				case False:
					fixedThen = true
					fixedThenValue = false
				}
				switch c.Else.(type) {
				case True:
					fixedElse = true
					fixedElseValue = true
				case False:
					fixedElse = true
					fixedElseValue = false
				}
				fixedCount := 0
				for i, v := range cond {
					if v.(bool) {
						if fixedThen {
							final[i] = fixedThenValue
							fixedCount++
						} else {
							if t[i] == nil {
								final[i] = false
								log.Printf("pkg/ops: invalid operation on line %d, you may need to add a null check, excluding row from output", i)
							}
							final[i] = t[i]
						}
					} else {
						if fixedElse {
							final[i] = fixedElseValue
							fixedCount++
						} else {
							if t[i] == nil {
								final[i] = false
								log.Printf("pkg/ops: invalid operation on line %d, you may need to add a null check, excluding row from output", i)
							}
							final[i] = e[i]
						}
					}
				}

				return final, BOOL
			}
		}
	}
}
func logical(operator Operator, fn func(bool, bool) bool) (map[uint32]interface{}, FieldType) {
	var (
		async = func(prop string, out chan Pair, op Operator) {
			m, t := op.Apply()
			if t != BOOL {
				log.Fatal("Logical statements must evaluate to a boolean")
			}
			out <- Pair{prop, m}
		}
		coll                         = make(chan Pair)
		final                        = make(map[uint32]interface{})
		l     map[uint32]interface{} = nil
		r     map[uint32]interface{} = nil
	)
	switch operator.(type) {
	case And:
		go async("l", coll, operator.(And).Lhs)
		go async("r", coll, operator.(And).Rhs)
	case Or:
		go async("l", coll, operator.(Or).Lhs)
		go async("r", coll, operator.(Or).Rhs)
	default:
		log.Fatal("logical operations must be And / Or")
	}

	for {
		select {
		case m := <-coll:
			if m.First == "l" {
				l = m.Second.(map[uint32]interface{})
			} else {
				r = m.Second.(map[uint32]interface{})
			}
			if l != nil && r != nil {
				for i, v := range l {
					if v == nil || r[i] == nil {
						final[i] = false
					} else {
						final[i] = fn(v.(bool), r[i].(bool))
					}
				}
				return final, BOOL
			}
		}
	}
}
func (or Or) Apply() (map[uint32]interface{}, FieldType) {
	return logical(or, func(first bool, second bool) bool {
		return first || second
	})
}
func (a And) Apply() (map[uint32]interface{}, FieldType) {
	return logical(a, func(first bool, second bool) bool {
		return first && second
	})
}
