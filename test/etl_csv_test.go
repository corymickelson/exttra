package test

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/loanpal-engineering/exttra/data"
	"github.com/loanpal-engineering/exttra/io/input"
	"github.com/loanpal-engineering/exttra/io/output"
	"github.com/loanpal-engineering/exttra/parser"
	. "github.com/loanpal-engineering/exttra/pkg"
	"github.com/loanpal-engineering/exttra/types"
	"github.com/loanpal-engineering/exttra/view"
	"github.com/pkg/errors"
)

func lpFileTest(i interface{}) {
	var (
		tb       testing.TB
		ok       bool
		node     Composer
		err      error = nil
		testFile       = "./etl_test_src-2019-08-01.csv"
		subject        = func(t testing.TB, fn func(Composer) (*bytes.Buffer, error)) {
			if _, err := fn(node); err != nil {
				t.Fatal(err)
			}
		}
	)
	tb, ok = i.(testing.TB)
	if !ok {
		log.Fatal("not a test or benchmark")
	}
	node, err = filingtracker(testFile)
	if err != nil {
		tb.Fatal(err)
	}
	switch i.(type) {
	case *testing.T:
		tb.(*testing.T).Run("SELECT WHERE MailedDt is NULL AND RecordedDt is NOT NULL", func(t *testing.T) {
			subject(t, recordedNotMailed)
		})
		node.(Editor).Reset()
		tb.(*testing.T).Run("SELECT WHERE if MailedDt is NOT NULL AND RecordedDt is NOT NULL and RecordedDt < MailedDt", func(t *testing.T) {
			subject(t, recordedBeforeMailed)
		})
		node.(Editor).Reset()
		tb.(*testing.T).Run("SELECT WHERE DocumentNbr is NULL AND RecordedDt is NOT NULL", func(t *testing.T) {
			subject(t, recordedWithoutDocNbr)
		})
	case *testing.B:
		tb.(*testing.B).Run("SELECT WHERE MailedDt is NULL AND RecordedDt is NOT NULL", func(b *testing.B) {
			subject(b, recordedNotMailed)
		})
		tb.(*testing.B).Run("SELECT WHERE if MailedDt is NOT NULL AND RecordedDt is NOT NULL and RecordedDt < MailedDt", func(t *testing.B) {
			subject(t, recordedBeforeMailed)
		})
		tb.(*testing.B).Run("SELECT WHERE DocumentNbr is NULL AND RecordedDt is NOT NULL", func(t *testing.B) {
			subject(t, recordedWithoutDocNbr)
		})
	}
}
func recordedNotMailed(table Composer) (*bytes.Buffer, error) {
	var (
		err         error = nil
		uccRecorded Composer
		uccMailed   Composer
		fixMailed   Composer
		fixRecorded Composer
	)
	if uccRecorded = table.Find("UCCR"); uccRecorded == nil {
		return nil, errors.New("filingtracker/supplement: UCCR not found")
	}
	if uccMailed = table.Find("UCCM"); uccMailed == nil {
		return nil, errors.New("filingtracker/supplement: UCCM not found")
	}
	if fixRecorded = table.Find("FIXR"); fixRecorded == nil {
		return nil, errors.New("filingtracker/supplement: FIXR not found")
	}
	if fixMailed = table.Find("FIXM"); fixMailed == nil {
		return nil, errors.New("filingtracker/supplement: FIXM not found")
	}
	null, _ := data.NewNode(nil, data.V(nil))
	type record struct {
		LoanId string
	}
	err = view.NewView(
		view.Select("ID"),
		view.From(table),
		view.Where(
			Or{
				And{
					Eq{uccMailed, null},
					Not{Eq{uccRecorded, null}}},
				And{
					Eq{fixMailed, null},
					Not{Eq{fixRecorded, null}}},
			}))
	mem := make([]interface{}, 0)
	var tmpl record
	if err = output.Mem(table, tmpl, &mem,
		output.Alias("ID", "LoanId")).Flush(); err != nil {
		goto exit
	}
	if len(mem) != 2 {
		err = errors.New(fmt.Sprint("etl_csv_test/recordedNotMailed: expected 2 matches but received %l", len(mem)))
		goto exit
	}
	for i := range mem {
		if record, ok := mem[i].(record); !ok {
			err = errors.New("etl_csv_test/recordedNotMailed: failure to cast to predefined shape")
			goto exit
		} else {
			matches := []string{"TT-15-o21514", "TT-05-o20294"}
			pass := false
			for _, id := range matches {
				if id == record.LoanId {
					pass = true
					break
				}
			}
			if !pass {
				err = errors.New("etl csv test: ID match(es) not found")
				goto exit
			}
		}
	}
exit:
	if err != nil {
		log.Print("RecordedNotMailed failed, see file for result")
		vOut := output.Csv(table,
			"/tmp/recordedNotMailed.csv",
			output.Alias("ID", "LoanID"))
		err = vOut.Flush()
	} else {

		return nil, err
	}
	return nil, nil
}
func recordedBeforeMailed(table Composer) (*bytes.Buffer, error) {
	var (
		err         error = nil
		uccRecorded Composer
		uccMailed   Composer
		fixMailed   Composer
		fixRecorded Composer
	)
	expected := []string{
		"TT-15-o20308",
		"TT-10-o20264",
		"TT-13-o20118",
		"TT-16-o20127",
		"TT-03-o20275",
		"TT-05-o20049",
		"TT-01-o20305",
		"TT-02-o20227",
		"TT-02-o20228",
		"TT-06-o20310",
		"TT-03-o20273",
		"TT-09-o20112",
		"TT-05-o20TU8",
		"TT-02-o20260",
		"TT-02-o20TU4",
		"TT-15-o20178",
		"TT-13-o20288",
		"TT-07-o20286",
		"TT-02-o20158",
		"TT-15-o20226",
		"TT-16-o20235",
		"TT-11-o20603",
		"TT-07-o20479",
		"TT-04-o20640",
		"TT-12-o20678",
		"TT-12-o20669",
		"TT-09-o20889",
		"TT-07-o20738",
		"TT-14-o20740",
		"TT-06-o21328",
		"TT-15-o21815",
		"TT-02-o2TU41",
		"TT-06-o20902",
		"TT-02-o22568",
		"TT-04-o20894",
		"TT-01-o20878",
		"TT-02-o23163",
		"TT-12-o23503",
		"TT-14-o20904",
		"TT-12-o23427",
		"TT-10-o21027",
		"TT-08-o22614",
		"TT-04-o23326",
		"TT-03-o22958",
		"TT-01-o22409",
		"TT-15-o21113",
		"TT-02-o22650",
		"TT-09-o22973",
		"TT-12-o22069",
		"TT-05-o22267",
		"TT-12-o22836",
		"TT-13-o21275",
		"TT-15-o23232",
		"TT-06-o22570",
		"TT-09-o23292",
		"TT-01-o22123",
		"TT-09-o23641",
		"TT-06-o22091",
		"TT-13-o22989",
		"TT-12-o22671",
		"TT-05-o22953",
		"TT-07-o21329",
		"TT-16-o23102",
		"TT-06-o22901",
		"TT-14-o22203",
		"TT-15-o23347",
		"TT-07-o21696",
		"TT-12-o21363",
		"TT-06-o23785",
		"TT-03-o23592",
		"TT-10-o22735",
		"TT-06-o23366",
		"TT-09-o24495",
		"TT-13-o22640",
		"TT-16-o23971",
		"TT-04-o23130",
		"TT-13-o24838",
		"TT-09-o24492",
		"TT-14-o23277",
		"TT-15-o22843",
		"TT-16-o24795",
		"TT-13-o24912",
		"TT-16-o24524",
		"TT-14-o24675",
		"TT-16-o23565",
		"TT-06-o23408",
		"TT-08-o23701",
		"TT-08-o24716",
		"TT-09-o24094",
		"TT-05-o24059",
		"TT-13-o25483",
		"TT-14-o24211",
		"TT-03-o22904",
		"TT-01-o25034",
		"TT-01-o25639",
		"TT-16-o25764",
		"TT-16-o23345",
		"TT-07-o23987",
		"TT-14-o24775",
		"TT-08-o25597",
		"TT-12-o25616",
		"TT-16-o23481",
		"TT-03-o25762",
		"TU-02-o26272",
		"TT-11-o24203",
		"TT-13-o24430",
		"TT-01-o24921",
		"TT-16-o25023",
		"TT-08-o24762",
		"TT-01-o25015",
		"TU-02-o26378",
		"TT-03-o24924",
		"TT-13-o23458",
		"TT-01-o25805",
		"TT-04-o25113",
		"TT-14-o25071",
		"TT-03-o24928",
		"TT-04-o25212",
		"TU-08-o26229",
		"TU-16-o26316",
		"TU-01-o26164",
		"TU-04-o26870",
		"TT-12-o25189",
		"TU-16-o26644",
		"TU-10-o26124",
		"TU-14-o26133",
		"TT-07-o25304",
		"TU-10-o26236",
		"TT-14-o25692",
		"TU-09-o26590",
		"TU-06-o26920",
		"TT-04-o24085",
		"TT-07-o25876",
		"TU-12-o26771",
		"TU-01-o26132",
		"TU-12-o26143",
		"TU-06-o27468",
		"TT-12-o25596",
		"TU-02-o26269",
		"TU-02-o26917",
		"TU-04-o28057",
		"TU-15-o26379",
		"TT-15-o24595",
		"TU-06-o27740",
		"TU-02-o27459",
		"TU-10-o27057",
		"TU-09-o26588",
		"TT-16-o25018",
		"TU-11-o276TT",
		"TU-11-o27889",
		"TU-03-o28697",
		"TU-03-o28177",
		"TU-12-o27251",
		"TT-12-o23652",
		"TT-02-o23715",
		"TT-11-o25047",
		"TT-02-o22999",
		"TT-14-o24059",
		"TT-14-o24282",
		"TT-05-o24645",
		"TT-11-o24480",
		"TT-12-o24340",
		"TT-03-o23499",
		"TT-01-o25066",
		"TT-04-o24615",
		"TT-14-o21423",
		"TT-08-o25246",
	}
	if uccRecorded = table.Find("UCCR"); uccRecorded == nil {
		return nil, errors.New("filingtracker/supplement: UCCR not found")
	}
	if uccMailed = table.Find("UCCM"); uccMailed == nil {
		return nil, errors.New("filingtracker/supplement: UCCM not found")
	}
	if fixRecorded = table.Find("FIXR"); fixRecorded == nil {
		return nil, errors.New("filingtracker/supplement: FIXR not found")
	}
	if fixMailed = table.Find("FIXM"); fixMailed == nil {
		return nil, errors.New("filingtracker/supplement: FIXM not found")
	}
	null, _ := data.NewNode(nil, data.V(nil))
	err = view.NewView(
		view.Select("ID", "UCCR", "UCCM", "FIXM", "FIXR"),
		view.From(table),
		view.Where(
			Or{
				If{
					And{Not{Eq{uccMailed, null}},
						Not{Eq{uccRecorded, null}}},
					Lt{uccRecorded, uccMailed},
					False{},
				},
				If{
					And{Not{Eq{fixMailed, null}},
						Not{Eq{fixRecorded, null}}},
					Lt{fixRecorded, fixMailed},
					False{},
				},
			},
		))
	type record struct {
		LoanId        string
		UCCRecordedDt time.Time
		UCCMailedDt   time.Time
		FIXRecordedDt time.Time
		FIXMailedDt   time.Time
	}
	mem := make([]interface{}, 0)
	var tmpl record
	if err = output.Mem(table, tmpl, &mem,
		output.Alias("ID", "LoanId"),
		output.Alias("UCCR", "UCCRecordedDt"),
		output.Alias("UCCM", "UCCMailedDt"),
		output.Alias("FIXR", "FIXRecordedDt"),
		output.Alias("FIXM", "FIXMailedDt")).Flush(); err != nil {
		goto exit
	}
	for i := range mem {

		var (
			item record
			ok   bool
		)
		item, ok = mem[i].(record)
		if !ok {
			err = errors.New(fmt.Sprintf("etl_csv_test/recordedBeforeMailed: unable to cast to shape for record %v", mem[i]))
			goto exit
		}
		pass := false
		for _, id := range expected {
			if id == item.LoanId {
				pass = true
				break
			}
		}
		if !pass {
			err = errors.New(fmt.Sprintf("etl_csv_test/recordedBeforeMailed: id %s not expected but present in result set", item.LoanId))
			goto exit
		}
	}
exit:
	if err != nil {
		log.Print("RecordedBeforeMailed failed, see file for result")
		vOut := output.Csv(table,
			"/tmp/recordedBeforeMailed.csv",
			output.Alias("ID", "LoanID"),
			output.Alias("UCCR", "UCCRecordedDt"),
			output.Alias("UCCM", "UCCMailedDt"),
			output.Alias("FIXR", "FIXRecordedDt"),
			output.Alias("FIXM", "FIXMailedDt"))
		if err = vOut.Flush(); err != nil {
			return nil, err
		}
	}

	return nil, err
}
func recordedWithoutDocNbr(table Composer) (*bytes.Buffer, error) {
	var (
		err         error = nil
		uccRecorded Composer
		uccDocNbr   Composer
		fixDocNbr   Composer
		fixRecorded Composer
		null, _     = data.NewNode(nil, data.V(nil))
	)
	expected := []string{
		"TT-09-o20064",
		"TT-13-o21533",
		"TT-16-o22750",
		"TT-15-o21830",
		"TT-02-o23681",
		"TT-14-o24430",
		"TT-02-o25295",
		"TT-01-o25389",
		"TT-1o-204654",
		"TT-12-o25060",
		"TT-12-o25421",
		"TU-05-o25961",
		"TT-02-o25671",
		"TT-03-o24714",
		"TT-04-o23766",
		"TT-01-o25543",
		"TT-1o-204782",
		"TT-01-o24840",
		"TT-14-o24866",
		"TU-14-o26909",
		"TU-15-o26462",
		"TU-02-o26326",
		"TT-04-o24620",
		"TU-02-o26734",
		"TU-05-o26494",
		"TT-08-o25801",
		"TT-02-o24395",
		"TU-01-o26757",
		"TU-02-o26644",
		"TT-04-o25210",
		"TU-08-o27231",
		"TU-08-o27056",
		"TU-05-o27563",
		"TU-04-o27999",
	}
	if uccRecorded = table.Find("UCCR"); uccRecorded == nil {
		return nil, errors.New("filingtracker/supplement: UCCR not found")
	}
	if uccDocNbr = table.Find("UCCD#"); uccDocNbr == nil {
		return nil, errors.New("filingtracker/supplement: UCCD# not found")
	}
	if fixRecorded = table.Find("FIXR"); fixRecorded == nil {
		return nil, errors.New("filingtracker/supplement: FIXR not found")
	}
	if fixDocNbr = table.Find("FIXD#"); fixDocNbr == nil {
		return nil, errors.New("filingtracker/supplement: FIXD# not found")
	}
	err = view.NewView(
		view.Select("ID", "UCCR", "UCCD#", "FIXR", "FIXD#"),
		view.From(table),
		view.Where(
			Or{
				And{
					Eq{uccDocNbr, null},
					Not{Eq{uccRecorded, null}}},
				And{
					Eq{fixDocNbr, null},
					Not{Eq{fixRecorded, null}}},
			}))
	type record struct {
		LoanId        string
		UCCRecordedDt time.Time
		UCCDocNbr     string
		FIXRecordedDt time.Time
		FIXDocNbr     string
	}
	var tmpl record
	mem := make([]interface{}, 0)
	if err = output.Mem(table, tmpl, &mem,
		output.Alias("ID", "LoanId"),
		output.Alias("UCCR", "UCCRecordedDt"),
		output.Alias("UCCD#", "UCCDocNbr"),
		output.Alias("FIXR", "FIXRecordedDt"),
		output.Alias("FIXD#", "FIXDocNbr")).Flush(); err != nil {
		goto exit
	}
	for i := range mem {
		if item, ok := mem[i].(record); ok {
			pass := false
			for _, id := range expected {
				if id == item.LoanId {
					pass = true
					break
				}
			}
			if !pass {
				err = errors.New(fmt.Sprintf("etl_csv_test/recordedWithoutDocNbr: item %s should not be in the result set", item.LoanId))
				goto exit
			}
		} else {
			err = errors.New(fmt.Sprintf("etl_csv_test/recordedWithoutDocNbr: can not cast item %v to user defined shape %v", tmpl, mem[i]))
			goto exit
		}
	}
exit:
	if err != nil {
		log.Print("RecordedWithoutDocNbr failed, see file for result")
		vOut := output.Csv(table,
			"/tmp/recordedWithoutDocNbr.csv",
			output.Alias("ID", "LoanID"),
			output.Alias("UCCR", "UCCRecordedDt"),
			output.Alias("UCCD#", "UCCDocNbr"),
			output.Alias("FIXR", "FIXRecordedDt"),
			output.Alias("FIXD#", "FIXDocNbr"))
		if err = vOut.Flush(); err != nil {
			return nil, err
		}
	}

	return nil, err
}
func filingtracker(testFile string) (Composer, error) {
	var (
		lp         = time.Date(2017, time.December, 1, 0, 0, 0, 0, time.UTC)
		now        = time.Now()
		root       Composer
		idFormat                  = regexp.MustCompile("[0-9]{2}-[0-9]{2}-[0-9]{6}")
		src        io.Reader      = nil
		err        error          = nil
		dateLimits FieldExtension = func(it interface{}) (interface{}, error) {
			t, ok := it.(time.Time)
			if !ok {
				return nil, errors.New(fmt.Sprintf("%view is not a timestamp/date", it))
			}
			if t.Unix() < lp.Unix() {
				return nil, errors.New(fmt.Sprintf("invalid date, date can not be before Loanpal began: %s", t.Format(time.RFC3339)))
			} else if t.Unix() > now.Unix() {
				return nil, errors.New(fmt.Sprintf("invalid date, date can not be in the future: %s", t.Format(time.RFC3339)))
			} else {
				return t, nil
			}
		}
		timeToDate output.CustomFormatter = func(it *string) {
			*it = (*it)[:10]
		}
		loanIdConverter FieldLevelConverter = func(id *string, _ ...interface{}) (interface{}, error) {
			if id == nil {
				return nil, errors.New("ID must not be null")
			} else if idFormat.MatchString(*id) {
				return idFormat.FindString(*id), nil
			} else {
				return fmt.Sprintf("%s-%s-%s", (*id)[0:2], (*id)[2:4], (*id)[4:]), nil
			}
		}
		loanIdToString StringifyField = func(it interface{}) *string {
			val := it.(string)
			return &val
		}
		explicitNull = "NULL"
		nullableDate = &Nullable{Allowed: true, ReplaceWith: &explicitNull}
		nullable     = &Nullable{Allowed: true}
		nonNull      = &Nullable{Allowed: false}
	)

	dateNilable, err := types.NewField(
		DATE,
		nullableDate,
		types.Extend(types.Convert, dateLimits),
	)
	loanId, err := types.NewField(
		CUSTOM,
		nonNull,
		types.Override(types.Convert, loanIdConverter),
		types.Override(types.ToString, loanIdToString))
	stringNilable, err := types.NewField(
		STRING,
		nullable)
	floatNilable, err := types.NewField(
		FLOAT32,
		nullable)

	reportDate := InterrogateDate(&testFile)
	def := types.NewSchema(
		types.Column("ID", loanId, true, true),
		types.Column("UCCM", dateNilable, true),
		types.Column("UCCC", stringNilable, true),
		types.Column("UCCC#", stringNilable, true),
		types.Column("UCCF", floatNilable, true),
		types.Column("UCCR", dateNilable, true),
		types.Column("UCCD#", stringNilable, true),
		types.Column("FIXM", dateNilable, true),
		types.Column("FIXC", stringNilable, true),
		types.Column("FIXC#", stringNilable, true),
		types.Column("FIXF", floatNilable, true),
		types.Column("FIXR", dateNilable, true),
		types.Column("FIXD#", stringNilable, true),
		types.Column("FIXRSent", dateNilable, true),
		types.Column("FIXRRec", dateNilable, true),
		types.Column("FIXRD#", stringNilable, true),
	)
	f, er := os.Open(testFile)
	if er != nil {
		return nil, err
	} else {
		src = bufio.NewReader(f)
	}

	subject := input.Csv(src, def)
	p := parser.NewParser(&subject)
	if err = p.Validate(nil); err != nil {
		return nil, err
	}

	root, err = p.Parse()
	dc := NewDC()
	defFile, _ := os.Create("/tmp/defects.csv")
	defs := dc.Report(0)
	dw := csv.NewWriter(defFile)
	if err = dw.WriteAll(defs); err != nil {
		log.Print(err.Error())
	}
	if err = output.Csv(root,
		"/tmp/table.csv",
		output.Alias("ID", "LoanID"),
		output.Alias("UCCM", "UCCMailedDt"),
		output.Format("UCCM", timeToDate),
		output.Alias("UCCC", "UCCComments"),
		output.Alias("UCCC#", "UCCCheckNbr"),
		output.Alias("UCCF", "UCCFee"),
		output.Alias("UCCR", "UCCRecordedDt"),
		output.Format("UCCR", timeToDate),
		output.Alias("UCCD#", "UCCDocumentNbr"),
		output.Alias("FIXM", "FixtureMailedDt"),
		output.Format("FIXM", timeToDate),
		output.Alias("FIXC", "FixtureComments"),
		output.Alias("FIXC#", "FixtureCheckNbr"),
		output.Alias("FIXF", "FixtureFee"),
		output.Alias("FIXR", "FixtureRecordedDt"),
		output.Format("FIXR", timeToDate),
		output.Alias("FIXD#", "FixtureDocumentNbr"),
		output.Alias("FIXRSent", "FixtureRefileSendDt"),
		output.Format("FIXRSent", timeToDate),
		output.Alias("FIXRRec", "FixtureRefileRecordedDt"),
		output.Format("FIXRRec", timeToDate),
		output.Alias("FIXRD#", "FixtureRefileDocumentNbr"),
		output.AddOn("ReportDate", func(args interface{}) *string {
			reportDate := args.(string)
			return &reportDate
		}, *reportDate),
	).Flush(); err != nil {
		return nil, err
	}
	return root, err
}
func TestExtractTransformCSV(t *testing.T) {
	// testFilingTracker(t)
	lpFileTest(t)
}
func BenchmarkFilingTacker(b *testing.B) {
	// testFilingTracker(b)
	lpFileTest(b)
}
