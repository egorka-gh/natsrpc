package main

import (
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
			resp := BonusResp{param.Accaunt, param.Total * 0.9, 10.0, "External diskont 10%"}
			e := r.natsCnn.Publish(reply, &resp)
			if e != nil {
				log.Printf("Cant publish respond  %v", e)
			}
		}
	})
	if e != nil {
		log.Printf("Cant Register %v; %v", "bonus", e)
	}
	r.Run()
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

	cnn.Publish("bigzz.api.bonus", &BonusReq{"00123", 100.11})
	var resp = new(BonusResp) //*BonusResp

	cnn.Request("bigzz.api.bonus", &BonusReq{"003", 220.0}, resp, 10*time.Second)
	log.Printf("Resp '%v'", resp)
	cnn.Flush()

	/*
		r.natsCnn.Publish("bigzz.api.exit", []byte("msg3"))
		r.natsCnn.Flush()
	*/
	if err := cnn.LastError(); err != nil {
		log.Fatal(err)
	}

	log.Println("Exit")

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
