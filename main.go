package main

import (
	"log"
	"time"

	nats "github.com/nats-io/go-nats"
)

func runrpc() {
	r := New("nats://demo.nats.io:4222", "bigzz.api")

	r.Register("ping", func(subject, reply string, req string) {
		if reply != "" {
			r.natsCnn.Publish(reply, "I'm ok. You say '"+req+"'")
		}
	})
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

	cnn.Publish("bigzz.api.ping", []byte("msg1"))
	var resp string
	resp = "No resp"
	cnn.Request("bigzz.api.ping", []byte("msg2"), &resp, 10*time.Second)
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
