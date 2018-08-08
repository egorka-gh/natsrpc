package main

import (
	"errors"
	"log"
	"reflect"

	"github.com/nats-io/go-nats"
)

//HandlerFunc is the standart callback to process message
// used signature handler := func(subject, reply string, o *obj)
//
// Handler is a specific callback used for Subscribe. It is generalized to
// an interface{}, but we will discover its format and arguments at runtime
// and perform the correct callback, including de-marshaling JSON strings
// back into the appropriate struct based on the signature of the Handler.
//
// Handlers are expected to have one of four signatures.
//
//	type person struct {
//		Name string `json:"name,omitempty"`
//		Age  uint   `json:"age,omitempty"`
//	}
//
//	handler := func(m *Msg)
//	handler := func(p *person)
//	handler := func(subject string, o *obj)
//	handler := func(subject, reply string, o *obj)
//
// These forms allow a callback to request a raw Msg ptr, where the processing
// of the message from the wire is untouched. Process a JSON representation
// and demarshal it into the given struct, e.g. person.
// There are also variants where the callback wants either the subject, or the
// subject and the reply subject.
type HandlerFunc nats.Handler

/*
// Context is the RPC call context
type Context struct {
	Subject string
	Reply   string
}
*/

/*Engine is the natsrpc instance
 */
type Engine struct {
	SubjectBase string
	url         string
	natsCnn     *nats.EncodedConn
	handlers    map[string]HandlerFunc
	exit        chan struct{}
}

/*New ctreats Engine
 */
func New(urls string, base string) *Engine {
	engine := &Engine{
		SubjectBase: base,
	}
	if urls == "" {
		urls = nats.DefaultURL
	}
	engine.url = urls
	log.Printf("Url '%v'", engine.url)

	return engine
}

//Register register rpc call
//cmd is the last prt of subj, if SubjectBase='bigzz.api' and cmd='getbonus' full subject = 'bigzz.api.getbonus'
//message body is json encoded struct (parametr 4 handler)
func (engine *Engine) Register(cmd string, handler HandlerFunc) error {
	if engine.natsCnn != nil {
		//TODO refactor to mutex
		return errors.New("can't register handler while nats running")
	}
	if handler == nil {
		return errors.New("nats: Handler required")
	}

	if reflect.TypeOf(handler).Kind() != reflect.Func {
		panic("nats: Handler needs to be a func")
	}
	if engine.handlers == nil {
		engine.handlers = make(map[string]HandlerFunc)
	}
	subj := engine.SubjectBase + "." + cmd
	log.Printf("Register '%v'", subj)
	engine.handlers[subj] = handler

	return nil
}

//Run start nats, subscribe rpc calls, blocks
func (engine *Engine) Run() error {

	if engine.handlers == nil {
		return errors.New("No handlers")
	}

	log.Print("Connecting nats ...")
	//start nats
	cnn, err := nats.Connect(engine.url)
	if err != nil {
		return err
	}
	engine.natsCnn, err = nats.NewEncodedConn(cnn, nats.JSON_ENCODER)
	if err != nil {
		return err
	}
	defer engine.natsCnn.Close()
	log.Print("Connected")

	//subscribe rpc's
	for subj, h := range engine.handlers {
		_, e := engine.natsCnn.Subscribe(subj, h)
		if e != nil {
			log.Printf("Can't subscribe: %v\n", e)
		}
	}
	log.Print("Subscribe rpc complited")

	engine.exit = make(chan struct{})
	<-engine.exit
	//TODO usub and so on
	return nil
}

//Stop blcking
func (engine *Engine) Stop() {

	close(engine.exit)
}

/*
func (engine *Engine) routMessage(msg *nats.Msg) {
	hendler := engine.handlers[msg.Subject]
	if hendler == nil {
		log.Fatalf("Not implemented: %v\n", msg.Subject)
	}
	//TODO use	msg.Reply?
	//TODO custom params parser or json

	//create context
	c := Context{Engine: engine, Message: msg}
	//call
	go hendler(&c)
}
*/
