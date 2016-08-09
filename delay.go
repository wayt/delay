package delay

import (
	"errors"
	"github.com/jrallison/go-workers"
	"github.com/mitchellh/mapstructure"
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
	Name string
	fv   reflect.Value // Kind() == reflect.Func
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

func (f *Function) Delay(args ...interface{}) (string, error) {

	return workers.Enqueue(queue, f.Name, args)
}

func (f *Function) DelayAt(at time.Time, args ...interface{}) (string, error) {

	return workers.EnqueueAt(queue, f.Name, at, args)
}

func (f *Function) DelayIn(in time.Duration, args ...interface{}) (string, error) {

	return f.DelayAt(time.Now().Add(in), args...)
}

func (f *Function) call(args ...interface{}) {

	ft := f.fv.Type()
	in := []reflect.Value{}

	if len(args) != ft.NumIn() {
		log.Printf("[%s]: bad arguments count, got %d, expect %d\n", f.Name, len(args), ft.NumIn())
		return
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
			panic(errv) // Will be catch by retry middleware
		}
	}
}

func SetQueue(q string) {
	queue = q
}
func Worker(concurrency int) {
	workers.Process(queue, handler, concurrency)
	workers.Run()
}

func handler(message *workers.Msg) {

	funcName := message.Get("class").MustString()

	fun, ok := funcs[funcName]
	if !ok {
		log.Printf("unknown function name [%s], ignoring...\n", funcName)
		return
	}

	var args []interface{}

	if i := message.Args().Interface(); i != nil {
		args = i.([]interface{})
	}

	fun.call(args...)
}
