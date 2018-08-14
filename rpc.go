package main

import (
	"errors"
	"log"
	"sync"

	"github.com/nats-io/go-nats"
)

//HandlerFunc is the standart callback to process message
type HandlerFunc func(*Runer)

type handlersChain []HandlerFunc

/*Engine is the natsrpc instance
 */
type Engine struct {
	SubjectBase string
	Enc         Encoder

	natsCnn *nats.Conn
	rpcsMu  sync.RWMutex
	rpcs    map[string]*Runer
	exit    chan struct{}
}

//Runer runs RPC's on concrete subscription
type Runer struct {
	mu       sync.Mutex
	engine   *Engine
	handlers handlersChain
	sub      *nats.Subscription
	msg      *nats.Msg
}

/*
//Context is the RPC call context
type Context struct {
	Enc Encoder

	msg    *nats.Msg
	engine *Engine
}

*/

/*New ctreats Engine
 */
func New(cnn *nats.Conn, base string) *Engine {
	engine := &Engine{
		SubjectBase: base,
		natsCnn:     cnn,
		Enc:         &JsonEncoder{},
	}

	return engine
}

//Register register rpc call's
//cmd is the last prt of subj, if SubjectBase='bigzz.api' and cmd='getbonus' full subject = 'bigzz.api.getbonus'
//message body is json encoded struct (parametr 4 handler)
func (engine *Engine) Register(cmd string, handler ...HandlerFunc) error {
	if engine.natsCnn == nil {
		return errors.New("rpc: has no connection")
	}
	if len(handler) == 0 {
		return errors.New("rpc: Handler required")
	}

	engine.rpcsMu.Lock()
	defer engine.rpcsMu.Unlock()

	if engine.rpcs == nil {
		engine.rpcs = make(map[string]*Runer)
	}
	subj := engine.SubjectBase + "." + cmd
	log.Printf("Register '%v'", subj)
	r, ok := engine.rpcs[subj]
	if !ok {
		//init rpc
		r = &Runer{engine: engine, handlers: handlersChain{}}
		engine.rpcs[subj] = r
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers = append(r.handlers, handler...)
	//subscribe if new rpc
	if !ok {
		sub, err := engine.natsCnn.Subscribe(subj, r.run)
		if err != nil {
			return err
		}
		r.sub = sub

	}
	return nil
}

//Run simple blocks
func (engine *Engine) Run() error {

	if engine.natsCnn == nil {
		return errors.New("Not connected")
	}
	/*
		if engine.rpcs == nil {
			return errors.New("No handlers")
		}
	*/

	log.Print("rpc: Started")

	engine.exit = make(chan struct{})
	for engine.exit != nil {
		select {
		case _, ok := <-engine.exit:
			if !ok {
				engine.exit = nil
			}
		}
	}

	//usubcribe
	for _, r := range engine.rpcs {
		if r.sub != nil {
			r.sub.Unsubscribe()
		}
	}
	//TODO waite for started runers

	//exit
	return nil
}

//Stop blocking
func (engine *Engine) Stop() {

	close(engine.exit)
}

//callback 4 nats msg
func (r *Runer) run(msg *nats.Msg) {
	log.Print("rpc: Runer.run")
	//TODO decode msg from msgp
	//create copy
	c := &Runer{
		msg:    msg,
		engine: r.engine,
	}
	r.mu.Lock()
	c.handlers = append(handlersChain{}, r.handlers...)
	r.mu.Unlock()

	//run in parallel
	for _, h := range c.handlers {
		go h(c)
	}
}

//Decode  decode msg body to obj using current encoder
func (r *Runer) Decode(obj interface{}) error {
	return r.engine.Enc.Decode(r.msg.Data, obj)
}

//Reply  encode msg body to obj using current encoder
// and  publish reply msg
func (r *Runer) Reply(obj interface{}) error {
	if r.msg.Reply == "" {
		return nil
	}
	//TODO encode msg.Data to msgp
	b, err := r.engine.Enc.Encode(obj)
	if err != nil {
		return err
	}
	return r.engine.natsCnn.Publish(r.msg.Reply, b)
}
