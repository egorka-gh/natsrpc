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
	runRPC(opts, "tst", []string{"cmd1", "cmd2"}, 3)

	//publish commands

	wg.Wait()

}

func runRPC(opts nats.Options, prefix string, cmd []string, handlers int) (*natsrpc.Engine, error) {
	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	rpc := natsrpc.New(nc, prefix)

	for _, v := range cmd {
		rpc.Register(v, func(m *natsrpc.M) {

		})
	}

	return rpc, nil
}
