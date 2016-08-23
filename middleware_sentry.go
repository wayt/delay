package delay

import (
	"fmt"
	"github.com/getsentry/raven-go"
	"github.com/wayt/go-workers"
	"runtime/debug"
)

type MiddlewareSentry struct{}

func (s *MiddlewareSentry) Call(queue string, message *workers.Msg, next func() error) (err error) {

	defer func() {
		if e := recover(); e != nil {
			err = workers.Fatal(fmt.Sprintf("%v", e))
			debug.PrintStack()
		}

		if err != nil && workers.IsFatal(err) {
			raven.CaptureError(err, nil)
		}
	}()

	err = next()

	return
}
