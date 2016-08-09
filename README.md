# Delay

Delay is a function scheduling addon to https://github.com/jrallison/go-workers

Mostly inspired by Google AppEngine delay package

## Example

examples/main.go:

```
package main

import (
	"flag"
	"fmt"
	"github.com/wayt/delay"
)

var mode = flag.String("mode", "worker", "Mode: `worker` or `producer`")
var server = flag.String("server", "192.168.99.100:6379", "Redis host")
var database = flag.String("database", "0", "Redis database")
var pool = flag.String("pool", "30", "Redis pool")
var process = flag.String("process", "1", "Worker unique process ID")

var sayHello = delay.Func("hello", func(name string) { fmt.Printf("Hello %s !\n", name) })

func main() {

	flag.Parse()

	delay.Configure(map[string]string{
		// location of redis instance
		"server": *server,
		// instance of the database
		"database": *database,
		// number of connections to keep open with redis
		"pool": *pool,
		// unique process id for this instance of workers (for proper recovery of inprogress jobs on crash)
		"process": *process,
	})

	if *mode == "worker" {
		delay.Worker(10)
	} else {
		sayHello.Delay("Bob")
	}
}
```
