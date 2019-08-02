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
		instance.coll = make([]*Defect, 0, 100)
		instance.enabled = true
		instance.Headers = []string{
			"Column",
			"Row",
			"Message",
		}
	})
	return instance
}

func (d *Defects) Count() int {
	return len(d.coll)
}
// Disable the global defects collector
func (d *Defects) Disable() {
	instance.enabled = false
}
func (d *Defects) Coll() []*Defect {
	return d.coll
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
	rows := make([][]string, 0, len(d.coll)+1)
	rows = append(rows, d.Headers)
	for _, v := range d.coll {
		row := make([]string, len(d.Headers))
		row[0] = v.Col
		row[1] = v.Row
		row[2] = v.Msg
		for k, vv := range v.Keys {
			for i, j := range rows[0] {
				if j == k {
					row[i] = vv
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
	defs := NewDC()
	if defs.(*Defects).enabled {
		checkRecord(d)
		defs.(*Defects).coll = append(defs.(*Defects).coll, d)
		if defs.(*Defects).exitInterrupt != nil {
			defs.(*Defects).exitInterrupt(defs.(*Defects).coll)
		}
		os.Exit(1)
	} else {
		log.Fatalf("defect: %s", d.Msg)
	}

}
