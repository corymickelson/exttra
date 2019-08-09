package pkg

import (
	"log"
	"os"
	"sync"
)

var (
	once     sync.Once
	instance *Defects
)

// Create or gets the global defects collection.
func NewDC() Defector {
	once.Do(func() {
		instance = new(Defects)
		instance.coll = make([]*Defect, 0)
		instance.enabled = true
		instance.Headers = []string{
			"Column",
			"Row",
			"Message",
		}
	})
	return instance
}

// Disable the global defects collector
func (d *Defects) Disable() {
	instance.enabled = false
}

// Get the current collection of defects.
// This is a reference to the collection.
// Any changes made to the results will be persisted to the Defect instance
func (d *Defects) Coll() *[]*Defect {
	return &d.coll
}
func (d *Defects) Count() int {
	return len(d.coll)
}

// Interrupt a FatalDefect to run the provided function
// with the global collection of defects before the
// process exits
func (d *Defects) ExitInterrupt(f func([]*Defect)) {
	d.exitInterrupt = f
}

// Generate a csv report of collected defects.
// If the schema defined column(s) as unique, these columns will
// be appended to the resulting file.
func (d *Defects) Report() [][]string {
	rows := make([][]string, 0, len(d.coll))
	for k := range d.coll[0].Keys {
		found := false
		for _, v := range d.Headers {
			if k == v {
				found = true
				break
			}
		}
		if !found {
			d.Headers = append(d.Headers, k)
		}
	}
	d.Headers = d.Headers[:len(d.Headers)]
	rows = append(rows, d.Headers)
	for _, v := range d.coll {
		row := make([]string, len(d.Headers))
		row[0] = v.Col
		row[1] = v.Row
		row[2] = v.Msg
		for k, vv := range v.Keys {
			for ii, j := range rows[0] {
				if j == k {
					row[ii] = vv
				}
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func (def *Defect) Error() string {
	return def.Msg
}

func checkRecord(d *Defect) {
	if d.Col == "" || d.Row == "" {
		log.Printf("Defects without column and row indices can not reference keys from input for results file")
	}
}

// Adds a record to the global defector object
// logging is NOT fatal, if a function uses LogDefect
// it's the responsibility of the function implementation/caller
// to set sentinel return value(s) and handle nils
func LogDefect(d *Defect) {
	if d.Keys == nil {
		d.Keys = map[string]string{}
	}
	defs := NewDC()
	if defs.(*Defects).enabled {
		checkRecord(d)
		defs.(*Defects).coll = append(defs.(*Defects).coll, d)
	} else {
		log.Printf("defect: %s", d.Msg)
	}
}

// A fatal defect is fatal! The process WILL exit.
// The defect will be added to the global defector object
// To capture collected defects before the process exits
// set the FatalExitInterrupt
func FatalDefect(d *Defect) {
	if d.Keys == nil {
		d.Keys = map[string]string{}
	}
	defs := NewDC().(*Defects)
	if defs.enabled {
		checkRecord(d)
		defs.coll = append(defs.coll, d)
		if defs.exitInterrupt != nil {
			defs.exitInterrupt(defs.coll)
		}
		os.Exit(1)
	} else {
		log.Fatalf("defect: %s", d.Msg)
	}

}
