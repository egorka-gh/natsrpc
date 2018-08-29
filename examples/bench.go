package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"natsrpc"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats/bench"
)

// Some sane defaults
const (
	DefaultNumMsgs = 10
	DefaultNumPubs = 10
	//DefaultNumSubs     = 100
	DefaultNumHandlers = 10
	DefaultMessageSize = 128
)

func usage() {
	log.Fatalf("Usage: bench [-s server (%s)] [-np NUM_PUBLISHERS] [-nh NUM_HANDLERS] [-n NUM_RPC] [-ms MESSAGE_SIZE] [-csv csvfile] <rpc-prefix> <reply-subject>\n", nats.DefaultURL)
}

var benchmark *bench.Benchmark

func main() {
	var urls = flag.String("s", nats.DefaultURL, "The nats server URLs (separated by comma)")
	var numPubs = flag.Int("np", DefaultNumPubs, "Number of Concurrent rpc Publishers")
	//var numSubs = flag.Int("ns", DefaultNumSubs, "Number of Concurrent Subscribers")
	var numHnds = flag.Int("nh", DefaultNumHandlers, "Number of rpc Handlers")
	var numMsgs = flag.Int("n", DefaultNumMsgs, "Number of rpc to Publish")
	var msgSize = flag.Int("ms", DefaultMessageSize, "Size of the message.")
	var csvFile = flag.String("csv", "", "Save bench data to csv file")

	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	args := flag.Args()
	if len(args) != 2 {
		usage()
	}

	if *numMsgs <= 0 {
		log.Fatal("Number of rpc should be greater than zero.")
	}
	if *numHnds <= 0 {
		log.Fatal("Number of rpc handlers should be greater than zero.")
	}

	// Setup the option block
	opts := nats.GetDefaultOptions()
	opts.Servers = strings.Split(*urls, ",")
	for i, s := range opts.Servers {
		opts.Servers[i] = strings.Trim(s, " ")
	}

	//TODO check params
	benchmark = bench.NewBenchmark("NATS", 2, *numPubs)

	var startwg sync.WaitGroup
	var donewg sync.WaitGroup
	calls := *numPubs * *numMsgs * *numHnds
	log.Printf("Expected rpc calls=%d\n", calls)

	donewg.Add(*numPubs + calls)

	// Run rpc Subscribers first
	startwg.Add(1)
	//for i := 0; i < *numSubs; i++ {
	go runSubscriber(&startwg, &donewg, opts, *numPubs, *numMsgs, *numHnds, *msgSize)
	//}
	startwg.Wait()

	//listen replies
	//TODO it's not in bench
	nc, err := opts.Connect()
	received := 0
	defer nc.Close()
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	nc.Subscribe(args[1], func(msg *nats.Msg) {
		received++
	})

	// Now Publishers
	startwg.Add(*numPubs)
	//pubCounts := bench.MsgsPerClient(*numMsgs, *numPubs)
	for i := 0; i < *numPubs; i++ {
		go runPublisher(&startwg, &donewg, opts, *numMsgs, *msgSize)
	}

	log.Printf("Starting benchmark [hndls=%d, msgs=%d, msgsize=%d, pubs=%d]\n", *numHnds, *numMsgs, *msgSize, *numPubs)

	startwg.Wait()
	donewg.Wait()

	benchmark.Close()

	fmt.Print(benchmark.Report())

	if len(*csvFile) > 0 {
		csv := benchmark.CSV()
		ioutil.WriteFile(*csvFile, []byte(csv), 0644)
		fmt.Printf("Saved metric data in csv file %s\n", *csvFile)
	}
	time.Sleep(3 * time.Second)
	log.Printf("Replies=%d\n", received)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func runPublisher(startwg, donewg *sync.WaitGroup, opts nats.Options, numMsgs int, msgSize int) {
	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	var cnn *nats.EncodedConn
	cnn, err = nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}

	defer cnn.Close()
	startwg.Done()

	args := flag.Args()
	//subj := "base." + args[0]
	subj := args[0]
	repl := args[1]
	var msg = randStringBytes(msgSize)

	start := time.Now()

	for i := 0; i < numMsgs; i++ {
		cnn.PublishRequest(fmt.Sprintf("%s%d", subj, i), repl, msg)
	}
	cnn.Flush()
	benchmark.AddPubSample(bench.NewSample(numMsgs, msgSize, start, time.Now(), nc))

	donewg.Done()
}

func runSubscriber(startwg, donewg *sync.WaitGroup, opts nats.Options, numPubs int, numMsgs int, numHnds int, msgSize int) {
	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	rpc := natsrpc.New(nc, "")

	args := flag.Args()
	subj := args[0]
	//repl := args[1]

	total := int64(numPubs * numMsgs * numHnds)
	done := int64(0)
	start := time.Now()

	for i := 0; i < numMsgs; i++ {
		topic := fmt.Sprintf("%s%d", subj, i)
		for j := 0; j < numHnds; j++ {
			rpc.Register(topic, func(r *natsrpc.M) {
				var param string
				if err := r.Decode(&param); err != nil {
					log.Fatal(err)
				}
				if err := r.Reply(param); err != nil {
					log.Fatal(err)
				}

				cnt := atomic.AddInt64(&done, 1)
				if cnt >= total {
					benchmark.AddSubSample(bench.NewSample(numPubs*numMsgs, msgSize, start, time.Now(), nc))
					log.Printf("Total Handler calls: %d\n", cnt)
					r.StopEngine()
				}
				donewg.Done()
			})
		}
	}

	/*
		nc.Subscribe(subj, func(msg *nats.Msg) {
			received++
			if received >= numMsgs {
				benchmark.AddSubSample(bench.NewSample(numMsgs, msgSize, start, time.Now(), nc))
				donewg.Done()
				nc.Close()
			}
		})
	*/
	nc.Flush()
	startwg.Done()

	rpc.Run()
	nc.Close()
}
