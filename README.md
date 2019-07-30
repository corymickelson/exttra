# Exttra
An extract transform library

## Brief
Extract data from csv, transform with built-in transformers or create your own transformation function.
Create views over data with built-in expressions: 

- Eq (==)
- Lt (<)
- Gt (>)
- Or (||)
- And (&&)
- Not (!)

## Example
Loading a csv, getting all records that have a mailed date and a recorded date but where recorded date is less than the mailed date.
In sql this might look like: ```sql SELECT * FROM file f WHERE f.MailedDate IS NOT NULL AND f.RecordedDate IS NOT NULL AND f.RecordedDate < f.MailedDate```

```go
var (
    table Composer
    err   error = nil
)
// Create a definition
def := types.NewSchema(
    types.Column("Loan ID", loanId, true, true),
    types.Column("Date UCC mailed", dateNilable, true),
    types.Column("UCC Comments", stringNilable, true),
    types.Column("UCC check #", stringNilable, true),
    types.Column("UCC fee (per filing)", floatNilable, true),
    types.Column("Date UCC recorded", dateNilable, true),
    types.Column("UCC document #", stringNilable, true),
    types.Column("Date FIXTURE mailed", dateNilable, true),
    types.Column("FIXTURE Comments", stringNilable, true),
    types.Column("FIXTURE check #", stringNilable, true),
    types.Column("FIXTURE fee (per filing)", floatNilable, true),
    types.Column("Date FIXTURE recorded", dateNilable, true),
    types.Column("FIXTURE document #", stringNilable, true),
    types.Column("Date FIXTURE REFILE sent", dateNilable, true),
    types.Column("Date FIXTURE REFILE recorded", dateNilable, true),
    types.Column("FIXTURE REFILE document #", stringNilable, true),
)
// load the actual file
f, er := os.Open(testFile)
if er != nil {
    return nil, err
} else {
    src = bufio.NewReader(f)
}
// Creat a new input source from the loaded file and the definition
subject := input.Csv(src, def)
p := parser.NewParser(&subject)
if err = p.Validate(nil); err != nil {
    return nil, err
}

table, err = p.Parse()
if err != nil {
    log.Fatal(err)
}
// Get nodes needed for statement
uccRecorded = table.Find("Date UCC recorded")
uccMailed = table.Find("Date UCC mailed")
fixRecorded = table.Find("Date FIXTURE recorded")
fixMailed = table.Find("Date FIXTURE mailed")

// Create a null node as a comparator
null, _ := data.NewNode(nil, data.V(nil))

err = view.NewView(
    view.Select("Loan ID", "Date UCC recorded", "Date UCC mailed", "Date FIXTURE mailed", "Date FIXTURE recorded"),
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
// Create a type to map results to
type record struct {
    LoanId        string
    UCCRecordedDt time.Time
    UCCMailedDt   time.Time
    FIXRecordedDt time.Time
    FIXMailedDt   time.Time
}
// mem is an out parameter used with Mem(ory) output
mem := make([]interface{}, 0)
var tmpl record
if err = output.Mem(table, tmpl, &mem,
    output.Alias("Loan ID", "LoanId"),
    output.Alias("Date UCC recorded", "UCCRecordedDt"),
    output.Alias("Date UCC mailed", "UCCMailedDt"),
    output.Alias("Date FIXTURE recorded", "FIXRecordedDt"),
    output.Alias("Date FIXTURE mailed", "FIXMailedDt")).Flush(); err != nil {
    log.Fatal(err.Error())
}
// Results are now loaded to mem in the shape of the record type
```
