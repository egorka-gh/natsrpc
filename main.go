package main

import (
	"log"
)

func main() {

	r, e := New("nats://demo.nats.io:4222", "bigzz.api")
	if e != nil {
		log.Fatalf("Can't connect: %v\n", e)
	}

	r.Serve("ping", pingH)
	r.Serve("exit", exitH)

	r.natsCnn.Publish("bigzz.api.ping", []byte("msg1"))
	r.natsCnn.Publish("bigzz.api.ping", []byte("msg2"))
	r.natsCnn.Flush()
	r.natsCnn.Publish("bigzz.api.exit", []byte("msg3"))

	r.natsCnn.Flush()
	if err := r.natsCnn.LastError(); err != nil {
		log.Fatal(err)
	}
	r.Run()

	log.Println("Exit")

}

func pingH(c *Context) {
	log.Println("ping from " + c.Message.Subject + ";msg " + string(c.Message.Data))
}

func exitH(c *Context) {
	log.Println("RPC Exit")

	c.Engine.Stop()
}
