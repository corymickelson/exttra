/*
Package defect provides a global (thread safe singleton) object to aggregate defect.Defect(s)
The Defector interface provides a single method [Report] to generate a csv file from the defects collected at runtime.
To get a reference to the global object use the [New] function.

def := defect.New() // returns global object

Helper methods are provided for two common error handlers: [LogDefect] and [FatalDefect].

A [LogDefect] stores the defect, and the coordinates to the defect in a collection, and then proceeds.
Log level defects must not be fatal. If the global defects are disabled, a Log level defect will fallback to
simply printing the message to std out.

ld := defect.LogDefect(&defect.Defect{
		Msg: "this is a message",
		Value: nil,
		Col: column index,
		Row: row index,
		Type: FieldType,
	})

A [FatalDefect] has the same signature as the [LogDefect] however on a fatal defect the process will be terminated.
To capture the global defects collection before exit an interrupt can be set on the global defect object.
This interrupt sets a function to be called before the process exits.

d := defect.New()
d.ExitInterrupt(func(coll []Defect) {
	// do something
	})

func bad() {
	...
	if err != nil {
		defect.FatalDefect(&defect.Defect{
			Msg: fmt.Sprintf("your message, error: %s", err.Error()),
		})
	}
}

 */
package defect
