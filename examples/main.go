package main

import (
	"log"
	"natsrpc"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/go-nats"
)

//CallData is the rpc data
type CallData struct {
	Cmd     string
	Client  int
	Request int
	Handler int
}

func main() {
	opts := nats.GetDefaultOptions()
	opts.Servers = []string{nats.DefaultURL}

	topic := "tst"
	cmds := []string{"cmd1", "cmd2"}
	clients := 3
	requests := 3
	handlers := 3
	echo := false

	// run rpc server
	wRPC := &sync.WaitGroup{}
	wRPC.Add(1)
	go runRPC(opts, topic, cmds, handlers, wRPC, echo)
	wRPC.Wait()

	//rpc responce counter
	resp := "resp"
	if len(topic) > 0 {
		resp = topic + "." + resp
	}
	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("main: Can't connect: %v\n", err)
	}
	cnn, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		log.Fatalf("main: Can't connect: %v\n", err)
	}
	defer cnn.Close()

	var cnt uint32
	var expect uint32
	expect = uint32(clients * requests * len(cmds) * handlers)
	wg := &sync.WaitGroup{}
	wg.Add(int(expect))

	cnn.Subscribe(resp, func(d *CallData) {
		if echo {
			log.Printf("main: Get: %v", d)
		}
		done := atomic.AddUint32(&cnt, 1)
		if done <= expect {
			wg.Done()
		}
	})

	//prepare rpc clients
	chanStart := make(chan struct{})
	for i := 0; i < clients; i++ {
		go runClient(opts, topic, cmds, resp, requests, i, chanStart)
	}
	//start clients
	time.Sleep(2 * time.Second)
	close(chanStart)

	//waite all responces
	if waitTimeout(wg, time.Second*7) {
		log.Println("main: Timed out")
	} else {
		log.Println("main: Finished")
	}

	time.Sleep(2 * time.Second)
	log.Printf("Expected %v, recived %v", expect, cnt)

	/*
		//try unregister rpc command
		d := CallData{Cmd: cmds[0]}
		cnn.PublishRequest(topic+".unregister", resp, &d)
		time.Sleep(2 * time.Second)
		expect = uint32(clients * requests * (len(cmds) - 1) * handlers)
		cnt = 0
		wg.Add(int(expect))
		//rerun rpc clients
		chanStart = make(chan struct{})
		for i := 0; i < clients; i++ {
			go runClient(opts, topic, cmds, resp, requests, i, chanStart)
		}
		time.Sleep(2 * time.Second)
		//start clients
		close(chanStart)
		//waite responces
		if waitTimeout(wg, time.Second*7) {
			log.Println("main: Timed out")
		} else {
			log.Println("main: Finished")
		}
		time.Sleep(2 * time.Second)
		log.Printf("Expected %v, recived %v", expect, cnt)
	*/

	//stop rpc
	cnn.PublishRequest(topic+".stop", resp, "stop it")
	time.Sleep(2 * time.Second)

	log.Println("main: Exit")
}

func runRPC(opts nats.Options, prefix string, cmd []string, handlers int, wg *sync.WaitGroup, echo bool) error {
	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("RPC Can't connect: %v\n", err)
	}
	defer nc.Close()
	rpc := natsrpc.New(nc, prefix)

	//register handlers
	for _, v := range cmd {
		for i := 0; i < handlers; i++ {
			func(i int, echo bool) {
				e := rpc.Register(v, func(m *natsrpc.M) {
					d := &CallData{}
					e := m.Decode(d)
					if e != nil {
						log.Printf("Can't decode %v, err: %v", m.RawData(), e.Error())
					}
					d.Handler = i
					if echo {
						log.Printf("RPC handler %v. Data %v", i, d)
					}
					e = m.Reply(d)
					if e != nil {
						log.Printf("Cant publish respond  %v", e.Error())
					}
				})
				if e != nil {
					log.Printf("RPC err: %v", e.Error())
				}
			}(i, echo)

		}
	}

	//register stop handler
	rpc.Register("stop", func(m *natsrpc.M) {
		log.Printf("RPC get stop command")
		d := &CallData{Cmd: "RPC: I'm stopping..."}
		if e := m.Reply(d); e != nil {
			log.Printf("Cant publish respond  %v", e)
		}
		m.StopEngine()
	})

	//register unregister handler
	rpc.Register("unregister", func(m *natsrpc.M) {
		d := &CallData{}
		if e := m.Decode(d); e != nil {
			log.Printf("Can't decode %v, err: %v", m.RawData(), e.Error())
		}
		log.Printf("RPC get unregister command for %s", d.Cmd)
		m.UnRegister(d.Cmd)

		d = &CallData{Cmd: "RPC: " + d.Cmd + " unregistered"}
		if e := m.Reply(d); e != nil {
			log.Printf("Cant publish respond  %v", e)
		}
	})

	wg.Done()
	rpc.Run()

	return nil
}

func runClient(opts nats.Options, prefix string, cmd []string, resp string, requests int, id int, start chan struct{}) error {
	nc, err := opts.Connect()
	if err != nil {
		log.Fatalf("Client: Can't connect: %v\n", err)
	}
	cnn, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		log.Fatalf("Client: Can't connect: %v\n", err)
	}
	defer cnn.Close()

	//waite start signal
	select {
	case <-start:
	}

	log.Printf("Start client %v", id)
	//run requests
	for i := 0; i < requests; i++ {
		for _, c := range cmd {
			topic := c
			if len(prefix) > 0 {
				topic = prefix + "." + topic
			}
			d := &CallData{Cmd: c, Client: id, Request: i}
			cnn.PublishRequest(topic, resp, d)
		}
	}

	return nil
}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
