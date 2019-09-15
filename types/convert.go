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

// Convert a field's value to an int64.
func IntConverter(in *string, _ ...interface{}) (interface{}, error) {
	rmSpecialChars := specialChars.ReplaceAllString(*in, "")
	out, err := strconv.ParseInt(rmSpecialChars, 10, 64)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("can not convert \"%s\" to int, parsing error: %s", *in, err.Error()))
	}
	return out, nil
}

// Convert a field's value to an float64.
func FloatConverter(in *string, _ ...interface{}) (interface{}, error) {
	rmSpecialChars := specialChars.ReplaceAllString(*in, "")
	out, err := strconv.ParseFloat(rmSpecialChars, 64)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("can not convert \"%s\" to float, parsing error: %s", *in, err.Error()))
	}
	return out, nil
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
