package pkg

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"time"
)

func InterrogateDate(key *string) *string {
	dateR := regexp.MustCompile(`[0-9]{2,4}-[0-9]{2}-[0-9]{2,4}`)
	if s := dateR.FindString(*key); len(s) > 0 {
		return &s
	} else {
		n := time.Now()
		y := strconv.Itoa(n.Year())
		m := n.Month().String()
		d := strconv.Itoa(n.Day())
		date := fmt.Sprintf("%s-%s-%s", y, m, d)
		return &date
	}
}

// Asserts if [i] is nil.
// Assertions also check for nilness of interface types
func IsNil(i interface{}) bool {
	return i == nil || (reflect.ValueOf(i).Kind() == reflect.Ptr && reflect.ValueOf(i).IsNil())
}
