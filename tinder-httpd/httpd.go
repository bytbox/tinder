package main

import (
	"flag"
	"net/http"
)

var (
	httpServer = flag.String("http", ":8080", "HTTP server address")
)

func main() {
	flag.Parse()
	err := http.ListenAndServe(*httpServer, nil)
	if err != nil {
		panic(err)
	}
}

