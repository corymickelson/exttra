package types

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/corymickelson/exttra/internal/defect"
	"github.com/corymickelson/exttra/pkg"
	"github.com/pkg/errors"

	"github.com/araddon/dateparse"
)

var (
	specialChars = regexp.MustCompile("[ $%,()a-zA-z]")
)

// Default ToString
func SimpleToString(it interface{}) *string {
	empty := ""
	if pkg.IsNil(it) {
		return &empty
	}
	var val string
	switch it.(type) {
	case string:
		val = it.(string)
	case bool:
		if it.(bool) == true {
			val = "true"
		} else {
			val = "false"
		}
	case time.Time:
		val = it.(time.Time).Format(time.RFC3339)
	case float64:
		val = fmt.Sprintf("%f", it.(float64))
	case float32:
		val = fmt.Sprintf("%f", it.(float32))
	case int:
		val = strconv.Itoa(it.(int))
	case int64:
		val = strconv.FormatInt(it.(int64), 10)
	case int32:
		val = strconv.Itoa(it.(int))
	case int16:
		val = strconv.Itoa(it.(int))
	case int8:
		val = strconv.Itoa(it.(int))
	default:
		defect.LogDefect(&defect.Defect{
			Msg: fmt.Sprintf("can not convert \"%v\" to string", it),
		})
		val = empty
	}
	return &val
}

// Try to convert a field's value to a boolean.
// Implements FieldLevelConverter.
func BoolConverter(in *string) (interface{}, error) {
	value := strings.TrimSpace(*in)
	var out *bool = nil
	t := true
	f := false
	isCurrency := regexp.MustCompile(`^\$[0-1]\.+`)
	// this may seem ridiculous but there has been many instances where a column is a boolean but
	// with values like $0 or $1
	if isCurrency.MatchString(value) {
		return nil, errors.New(fmt.Sprintf("can not convert \"%s\" to boolean", *in))
	}
	if strings.ToLower(value) == "true" || strings.ToLower(value) == "false" {
		if strings.ToLower(value) == "true" {
			out = &t
		} else {
			out = &f
		}
	}
	if strings.ToLower(value) == "yes" || strings.ToLower(value) == "no" {
		if strings.ToLower(value) == "yes" {
			out = &t
		} else {
			out = &f
		}
	}
	if value == "0" || value == "1" {
		if strings.ToLower(value) == "1" {
			out = &t
		} else {
			out = &f
		}
	}
	return out, nil
}

// Try to convert a field's value to a float64.
// Implements FieldLevelConverter.
func IntConverter(in *string) (interface{}, error) {
	rmSpecialChars := specialChars.ReplaceAllString(*in, "")
	out, err := strconv.ParseInt(rmSpecialChars, 10, 64)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("can not convert \"%s\" to int, parsing error: %s", *in, err.Error()))
	}
	return out, nil
}

// Try to convert a field's value to a float64.
// Implements FieldLevelConverter.
func FloatConverter(in *string) (interface{}, error) {
	rmSpecialChars := specialChars.ReplaceAllString(*in, "")
	out, err := strconv.ParseFloat(rmSpecialChars, 64)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("can not convert \"%s\" to float, parsing error: %s", *in, err.Error()))
	}
	return out, nil
}

// Try to convert a field's value to a time.Time
// Implements FieldLevelConverter.
func DateTimeConverter(in *string) (interface{}, error) {
	if t, err := dateparse.ParseAny(strings.TrimSpace(*in)); err != nil {
		return nil, err
	} else {
		return t, nil
	}
}
