package main

import (
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/bezrukov/queue/server"
	"github.com/bezrukov/queue/web"
)

var (
	dirname = flag.String("dirname", "", "The directory where to put all the data")
	inmem   = flag.Bool("inmem", false, "Whether or not use in-memory storage instead of a disk-based one")
	port    = flag.Uint("port", 8080, "Network port to listen on")
)

func main() {
	flag.Parse()

	var backend web.Storage

	if *inmem {
		backend = &server.InMemory{}
	} else {
		if *dirname == "" {
			log.Fatal("The flag `--dirname` must be provided")
		}

		filename := filepath.Join(*dirname, "test.file")
		fp, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			log.Fatalf("Could not create test file %q: %s", filename, err)
		}
		if err := os.Remove(filename); err != nil {
			log.Fatalf("could not remove test file: %s", err)
		}
		fp.Close()
		os.Remove(fp.Name())

		backend = server.NewOnDisk(*dirname)
	}

	s := web.NewServer(backend, *port)

	log.Printf("Listening connections on: %d", *port)
	s.Serve()
}
