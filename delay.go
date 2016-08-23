package delay

import (
	"errors"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/wayt/go-workers"
	"log"
	"reflect"
	"time"
)

var (
	funcs     = make(map[string]*Function)
	errorType = reflect.TypeOf((*error)(nil)).Elem()
	queue     = "delay"
)

func Configure(options map[string]string) {
	workers.Configure(options)
}

type Function struct {
	Name       string
	fv         reflect.Value // Kind() == reflect.Func
	retryCount int
	interval   int
}

func Func(name string, i interface{}) *Function {

	f := &Function{
		Name: name,
		fv:   reflect.ValueOf(i),
	}

	t := f.fv.Type()
	if t.Kind() != reflect.Func {
		panic(errors.New("not a function"))
	}

	funcs[name] = f

	return f
}

func (f *Function) RetryCount(count int) *Function {
	f.retryCount = count
	return f
}

func (f *Function) Interval(sec int) *Function {
	f.interval = sec
	return f
}

func (f *Function) Delay(args ...interface{}) (string, error) {

	return f.DelayAt(time.Now(), args...)
}

func (f *Function) DelayAt(at time.Time, args ...interface{}) (string, error) {

	return workers.EnqueueWithOptions(queue, f.Name, args, workers.EnqueueOptions{
		At:            float64(at.UnixNano()) / workers.NanoSecondPrecision,
		Retry:         f.retryCount > 0,
		RetryInterval: f.interval,
	})
}

func (f *Function) DelayIn(in time.Duration, args ...interface{}) (string, error) {

	return f.DelayAt(time.Now().Add(in), args...)
}

func (f *Function) call(args ...interface{}) error {

	ft := f.fv.Type()
	in := []reflect.Value{}

	if len(args) != ft.NumIn() {
		err := workers.Fatalf("[%s]: bad arguments count, got %d, expect %d", f.Name, len(args), ft.NumIn())
		log.Println(err.Error())
		return err
	}

	if ft.NumIn() > 0 {
		for i, arg := range args {
			var v reflect.Value
			if arg != nil {

				paramType := ft.In(i)

				tmp := reflect.New(paramType)
				mapstructure.Decode(arg, tmp.Interface())

				v = tmp.Elem()
			} else {
				// Task was passed a nil argument, so we must construct
				// the zero value for the argument here.
				n := len(in) // we're constructing the nth argument
				var at reflect.Type
				if !ft.IsVariadic() || n < ft.NumIn()-1 {
					at = ft.In(n)
				} else {
					at = ft.In(ft.NumIn() - 1).Elem()
				}
				v = reflect.Zero(at)
			}
			in = append(in, v)
		}
	}

	out := f.fv.Call(in)

	if n := ft.NumOut(); n > 0 && ft.Out(n-1) == errorType {
		if errv := out[n-1]; !errv.IsNil() {
			return errv.Interface().(error)
		}
	}

	return nil
}

func SetQueue(q string) {
	queue = q
}
func Worker(concurrency int, mids ...workers.Action) {
	workers.Process(queue, handler, concurrency, mids...)
	workers.Run()
}

func handler(message *workers.Msg) error {

	funcName := message.Get("class").MustString()

	fun, ok := funcs[funcName]
	if !ok {
		return fmt.Errorf("unknown function name [%s], ignoring...\n", funcName)
	}

	var args []interface{}

	if i := message.Args().Interface(); i != nil {
		args = i.([]interface{})
	}

	if err := fun.call(args...); err != nil {
		return err
	}

	return nil
}
