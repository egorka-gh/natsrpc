package main

import (
	"log"

	"github.com/nats-io/go-nats"
)

/*HandlerFunc is the standart callback to process message
 */
type HandlerFunc func(*Context)

// Context is the RPC call context
type Context struct {
	Engine  *Engine
	Message *nats.Msg
}

/*Engine is the natsrpc instance
 */
type Engine struct {
	SubjectBase string
	natsCnn     *nats.Conn
	handlers    map[string]HandlerFunc
	exit        chan struct{}
}

/*New ctreats Engine and connects to nats
 */
func New(urls string, base string) (*Engine, error) {
	engine := &Engine{
		SubjectBase: base,
	}
	if urls == "" {
		urls = nats.DefaultURL
	}
	log.Printf("Url '%v'", urls)
	var err error
	engine.natsCnn, err = nats.Connect(urls)
	if err != nil {
		//log.fatal("Can't connect: %v\n", err)
		return nil, err
	}

	return engine, nil
}

//Serve implement rpc call
//cmd is the last prt of subj, if SubjectBase='bigzz.api' and cmd='getbonus' full subject = 'bigzz.api.getbonus'
//message body is the list of parametrs
func (engine *Engine) Serve(cmd string, hendler HandlerFunc) {
	if engine.handlers == nil {
		engine.handlers = make(map[string]HandlerFunc)
	}
	subj := engine.SubjectBase + "." + cmd
	engine.handlers[subj] = hendler
	_, e := engine.natsCnn.Subscribe(subj, engine.routMessage)
	if e != nil {
		log.Fatalf("Can't connect: %v\n", e)
	}

}

//Run blocks execution
func (engine *Engine) Run() {
	engine.exit = make(chan struct{})
	<-engine.exit
	//TODO usub and so on
}

//Stop blcking
func (engine *Engine) Stop() {

	close(engine.exit)
}

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
