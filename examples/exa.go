package main

import (
	"errors"
	"log"
	"natsrpc"
	"sync"
	"time"

	nats "github.com/nats-io/go-nats"
)

func runrpc(c *nats.Conn, wg *sync.WaitGroup) {
	rr := natsrpc.New(c, "bigzz.api")

	e := rr.Register("bonus", func(r *natsrpc.M) {
		param := &BonusReq{}
		e := r.Decode(param)
		if e != nil {
			log.Printf("Can't decode %v, err: %v", r.RawData(), e)
		}
		log.Printf("RPC1. Get %s -> %s.\n Param %v; %v", r.Subject(), r.ReplyTopic(), param.Accaunt, param.Total)
		resp := &BonusResp{param.Accaunt, param.Total * 0.9, 10.0, "RPC1. External discount 10%"}
		r.Reply(resp)
		if e != nil {
			log.Printf("Cant publish respond  %v", e)
		}
	},
		func(r *natsrpc.M) {
			param := &BonusReq{}
			e := r.Decode(param)
			if e != nil {
				log.Printf("Can't decode %v, err: %v", r.RawData(), e)
			}
			log.Printf("RPC2. Get %s -> %s.\n Param %v; %v", r.Subject(), r.ReplyTopic(), param.Accaunt, param.Total)
			resp := &BonusResp{param.Accaunt, param.Total - param.Total*0.9, 10.0, "RPC2. Cashback"}
			r.Reply(resp)
			if e != nil {
				log.Printf("Cant publish respond  %v", e)
			}
		})
	if e != nil {
		log.Printf("Cant Register %v; %v", "bonus", e)
	}

	e = rr.Register("bonus", func(r *natsrpc.M) {
		param := &BonusReq{}
		e := r.Decode(param)
		if e != nil {
			log.Printf("Can't decode %v, err: %v", r.RawData(), e)
		}
		log.Printf("RPC3. Param %v; %v", param.Accaunt, param.Total)
		resp := &BonusResp{param.Accaunt, param.Total, 0, "RPC3."}
		r.Reply(resp)
		if e != nil {
			log.Printf("Cant publish respond  %v", e)
		}
	})
	if e != nil {
		log.Printf("Cant Register %v; %v", "bonus", e)
	}

	e = rr.Register("quit", func(r *natsrpc.M) {
		log.Print("Stopping rpc....")
		r.Reply("I'm stopping")
		r.StopEngine()
	})
	if e != nil {
		log.Printf("Cant Register %v; %v", "quit", e)
	}
	wg.Done()
	rr.Run()
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
	c, err := nats.Connect("nats://demo.nats.io:4222")
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go runrpc(c, wg)
	wg.Wait()
	//r.Serve("exit", exitH)

	var cnn *nats.EncodedConn
	cnn, err = nats.NewEncodedConn(c, nats.JSON_ENCODER)
	if err != nil {
		log.Fatalf("Can't connect: %v\n", err)
	}
	defer cnn.Close()

	log.Println("main: Simple Publish ")
	err = cnn.Publish("bigzz.api.bonus", &BonusReq{"00123", 100.11})
	if err != nil {
		log.Printf("main: Publish err: %v\n", err)
	}

	log.Println("main: Request")
	var resp = new(BonusResp) //*BonusResp
	err = cnn.Request("bigzz.api.bonus", &BonusReq{"003", 220.0}, resp, 10*time.Second)
	if err != nil {
		log.Printf("main: Publish err: %v\n", err)
	}
	log.Printf("main: Resp '%v'", resp)

	ch := make(chan bool)

	cnn.PublishRequest("bigzz.api.quit", "bigzz.api.quit.resp", "enough")

	cnn.Subscribe("bigzz.api.quit.resp", func(msg *nats.Msg) {
		log.Printf("main: Get: %v; Body %s", msg.Subject, msg.Data)
		ch <- true
	})

	if err := WaitTime(ch, 10*time.Second); err != nil {
		log.Println("Failed to receive message")
	}

	if err := cnn.LastError(); err != nil {
		log.Fatal(err)
	}

	time.Sleep(7 * time.Second)
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
