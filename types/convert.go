package types

import (
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/loanpal-engineering/exttra/pkg"
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
			val = "1"
		} else {
			val = "0"
		}
	case time.Time:
		val = it.(time.Time).Format(time.RFC3339)
	case float64:
		val = fmt.Sprint(it.(float64))
	case float32:
		val = fmt.Sprint(it.(float32))
	case uint:
		val = fmt.Sprint(it.(uint))
	case uint64:
		val = fmt.Sprint(it.(uint64))
	case uint32:
		val = fmt.Sprint(it.(uint32))
	case uint16:
		val = fmt.Sprint(it.(uint16))
	case uint8:
		val = fmt.Sprint(it.(uint8))
	case int:
		val = fmt.Sprint(it.(int))
	case int64:
		val = fmt.Sprint(it.(int64))
	case int32:
		val = fmt.Sprint(it.(int32))
	case int16:
		val = fmt.Sprint(it.(int16))
	case int8:
		val = fmt.Sprint(it.(int8))
	default:
		pkg.LogDefect(pkg.Defect{
			Msg: fmt.Sprintf("can not convert \"%v\" to string", it),
		})
		val = empty
	}
	return &val
}

// Try to convert a field's value to a boolean.
func BoolConverter(in *string, _ ...interface{}) (interface{}, error) {
	value := strings.TrimSpace(*in)
	isCurrency := regexp.MustCompile(`^\$[0-1]\.+`)
	// this may seem ridiculous but there has been many instances where a column is a boolean but
	// with values like $0 or $1
	if isCurrency.MatchString(value) {
		return nil, errors.New(fmt.Sprintf("can not convert \"%s\" to boolean", *in))
	}
	if strings.ToLower(value) == "true" || strings.ToLower(value) == "false" {
		if strings.ToLower(value) == "true" {
			return true, nil
		} else {
			return false, nil
		}
	}
	if strings.ToLower(value) == "yes" || strings.ToLower(value) == "no" {
		if strings.ToLower(value) == "yes" {
			return true, nil
		} else {
			return false, nil
		}
	}
	if value == "0" || value == "1" {
		if strings.ToLower(value) == "1" {
			return true, nil
		} else {
			return false, nil
		}
	}
	return false, errors.New(fmt.Sprintf("types/convert: Unable to parse %s to bool", value))
}

func int8Converter(in *string, _ ...interface{}) (out interface{}, err error) {
	var base int64
	if base, err = strconv.ParseInt(specialChars.ReplaceAllString(*in, ""), 10, 8); err != nil {
		return
	}
	out = int8(base)
	return
}
func int16Converter(in *string, _ ...interface{}) (out interface{}, err error) {
	var base int64
	if base, err = strconv.ParseInt(specialChars.ReplaceAllString(*in, ""), 10, 16); err != nil {
		return
	}
	out = int16(base)
	return
}
func int32Converter(in *string, _ ...interface{}) (out interface{}, err error) {
	var base int64
	if base, err = strconv.ParseInt(specialChars.ReplaceAllString(*in, ""), 10, 32); err != nil {
		return
	}
	out = int32(base)
	return
}
func int64Converter(in *string, _ ...interface{}) (out interface{}, err error) {
	var base int64
	if base, err = strconv.ParseInt(specialChars.ReplaceAllString(*in, ""), 10, 64); err != nil {
		return
	}
	out = base
	return
}
func uint8Converter(in *string, _ ...interface{}) (out interface{}, err error) {
	var base uint64
	if base, err = strconv.ParseUint(specialChars.ReplaceAllString(*in, ""), 10, 8); err != nil {
		return
	}
	out = uint8(base)
	return
}
func uint16Converter(in *string, _ ...interface{}) (out interface{}, err error) {
	var base uint64
	if base, err = strconv.ParseUint(specialChars.ReplaceAllString(*in, ""), 10, 16); err != nil {
		return
	}
	out = uint16(base)
	return
}
func uint32Converter(in *string, _ ...interface{}) (out interface{}, err error) {
	var base uint64
	if base, err = strconv.ParseUint(specialChars.ReplaceAllString(*in, ""), 10, 32); err != nil {
		return
	}
	out = uint32(base)
	return
}
func uint64Converter(in *string, _ ...interface{}) (out interface{}, err error) {
	var base uint64
	if base, err = strconv.ParseUint(specialChars.ReplaceAllString(*in, ""), 10, 64); err != nil {
		return
	}
	out = base
	return
}

// Convert a field's value to an int64.
func IntConverter(t pkg.FieldType) pkg.FieldLevelConverter {
	switch t {
	case pkg.INT8:
		return int8Converter
	case pkg.INT16:
		return int16Converter
	case pkg.INT:
		fallthrough
	case pkg.INT32:
		return int32Converter
	case pkg.INT64:
		return int64Converter
	case pkg.UINT8:
		return uint8Converter
	case pkg.UINT16:
		return uint16Converter
	case pkg.UINT:
		fallthrough
	case pkg.UINT32:
		return uint32Converter
	case pkg.UINT64:
		return uint64Converter
	default:
		panic(fmt.Sprintf("types/convert: Int %s not supported", t.String()))
	}
}

// Convert a field's value to an float64.
func float32Converter(in *string, _ ...interface{}) (out interface{}, err error) {
	var base float64
	rmSpecialChars := specialChars.ReplaceAllString(*in, "")
	if base, err = strconv.ParseFloat(rmSpecialChars, 32); err != nil {
		return
	} else {
		out = float32(base)
	}
	return out, err
}

// Convert a field's value to an float64.
func float64Converter(in *string, _ ...interface{}) (out interface{}, err error) {
	var base float64
	rmSpecialChars := specialChars.ReplaceAllString(*in, "")
	if base, err = strconv.ParseFloat(rmSpecialChars, 64); err != nil {
		return
	} else {
		out = base
	}
	return out, err
}

// Convert a field's value to an float64.
func FloatConverter(t pkg.FieldType) pkg.FieldLevelConverter {
	switch t {
	case pkg.FLOAT32:
		return float32Converter
	case pkg.FLOAT:
		fallthrough
	case pkg.FLOAT64:
		return float64Converter
	default:
		panic(fmt.Sprintf("types/convert: float %s not supported", t.String()))
	}

}

// Convert a field's value to an time instance.
func DateTimeConverter(in *string, retry ...interface{}) (interface{}, error) {
	if t, err := dateparse.ParseAny(strings.TrimSpace(*in)); err != nil {
		if len(retry) > 0 && retry[0].(bool) {
			log.Printf("types/convert: unable to parse %s to a known date/time", *in)
			return nil, err
		}
		log.Printf("types/convert: unable to parse %s to a known date/time format. "+
			"Making final attempt", *in)
		monDayYearRx := regexp.MustCompile("[0-9]{2}[-][0-9]{2}[-][0-9]{4}")
		if monDayYearRx.MatchString(*in) {
			var (
				candidate string
				attempt   string
				segments  []string
			)
			candidate = monDayYearRx.FindString(*in)
			segments = strings.Split(candidate, "-")
			attempt = fmt.Sprintf("%s-%s-%s", segments[2], segments[0], segments[1])
			return DateTimeConverter(&attempt, true)
		} else {
			return nil, err
		}
	} else {
		return t, nil
	}
}
