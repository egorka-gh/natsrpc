package main

import (
	"log"
	"natsrpc"
	"sync"

	"github.com/nats-io/go-nats"
)

func main() {
	opts := nats.GetDefaultOptions()
	opts.Servers = []string{nats.DefaultURL}

	var wg sync.WaitGroup
	// run rpc
	runRPC(opts, &wg)

	//publish commands

	wg.Wait()

}

func runRPC(opts nats.Options, wg *sync.WaitGroup) error {
	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	rpc := natsrpc.New(nc, "test")

	return nil
}
