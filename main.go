package main

import (
	"errors"
	"log"
	"time"

	nats "github.com/nats-io/go-nats"
)

func runrpc() {
	r := New("nats://demo.nats.io:4222", "bigzz.api")

	e := r.Register("bonus", func(subject, reply string, param *BonusReq) {
		log.Print("Get req " + subject + " -> " + reply)
		log.Printf("Param %v; %v", param.Accaunt, param.Total)
		if reply != "" {
			resp := BonusResp{param.Accaunt, param.Total * 0.9, 10.0, "External discount 10%"}
			e := r.natsCnn.Publish(reply, &resp)
			if e != nil {
				log.Printf("Cant publish respond  %v", e)
			}
		}
	})
	if e != nil {
		log.Printf("Cant Register %v; %v", "bonus", e)
	}

	e = r.Register("quit", func(msg *nats.Msg) {
		log.Printf("Get: %v; Inbox: %v; Body %v", msg.Subject, msg.Reply, msg.Data)
		log.Print("Stopping rpc....")
		if msg.Reply != "" {
			e := r.natsCnn.Publish(msg.Reply, "I'm stopping")
			if e != nil {
				log.Printf("Cant publish %v", e)
			}
		}
		r.Stop()
	})
	if e != nil {
		log.Printf("Cant Register %v; %v", "quit", e)
	}

	r.Run()
}

//WaitTime Wait for a chan with a timeout.
func WaitTime(ch chan bool, timeout time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
	}
	return errors.New("timeout")
}

func main() {

	go runrpc()
	//r.Serve("exit", exitH)

	c, err := nats.Connect("nats://demo.nats.io:4222")
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	var cnn *nats.EncodedConn
	cnn, err = nats.NewEncodedConn(c, nats.JSON_ENCODER)
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	defer cnn.Close()

	cnn.Publish("bigzz.api.bonus", &BonusReq{"00123", 100.11})
	cnn.Flush()

	var resp = new(BonusResp) //*BonusResp
	cnn.Request("bigzz.api.bonus", &BonusReq{"003", 220.0}, resp, 10*time.Second)
	log.Printf("main: Resp '%v'", resp)

	ch := make(chan bool)

	cnn.PublishRequest("bigzz.api.quit", "bigzz.api.quit.resp", "enough")

	cnn.Subscribe("bigzz.api.quit.resp", func(msg *nats.Msg) {
		log.Printf("main: Get: %v; Body %v", msg.Subject, msg.Data)
		ch <- true
	})

	if err := WaitTime(ch, 10*time.Second); err != nil {
		log.Println("Failed to receive message")
	}

	if err := cnn.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Println("main: Exit")

}

/*
func pingH(c *Context) {
	log.Println("ping from " + c.Message.Subject + ";msg " + string(c.Message.Data))
}

func exitH(c *Context) {
	log.Println("RPC Exit")

	c.Engine.Stop()
}
*/
