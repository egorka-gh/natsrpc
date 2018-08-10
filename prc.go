package main

import (
	"errors"
	"log"
	"sync"

	"github.com/nats-io/go-nats"
)

//HandlerFunc is the standart callback to process message
type HandlerFunc func(*Context)

type handlersChain []HandlerFunc

/*Engine is the natsrpc instance
 */
type Engine struct {
	SubjectBase string
	Enc         Encoder

	natsCnn *nats.Conn
	rpcsMu  sync.RWMutex
	rpcs    map[string]rpc
	exit    chan struct{}
}

type rpc struct {
	mu       sync.Mutex
	handlers handlersChain
	engine   *Engine
}

//Context is the RPC call context
type Context struct {
	Enc Encoder

	msg    *nats.Msg
	engine *Engine
}

func (r *rpc) run(msg *nats.Msg) {
	//TODO create context
	//TODO decode msg from msgp
	c := &Context{
		msg:    msg,
		engine: r.engine,
		Enc:    r.engine.Enc,
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, h := range r.handlers {
		go h(c)
	}
}

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
		engine.rpcs = make(map[string]rpc)
	}
	subj := engine.SubjectBase + "." + cmd
	log.Printf("Register '%v'", subj)
	r, ok := engine.rpcs[subj]
	if !ok {
		//init rpc
		r = rpc{engine: engine}
		engine.rpcs[subj] = r
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers = append(r.handlers, handler...)
	//subscribe if new rpc
	if !ok {
		if _, err := engine.natsCnn.Subscribe(subj, r.run); err != nil {
			return err
		}

	}
	return nil
}

//Run simple blocks
func (engine *Engine) Run() error {

	if engine.natsCnn == nil {
		return errors.New("Not connected")
	}
	if engine.rpcs == nil {
		return errors.New("No handlers")
	}

	log.Print("rpc: Started")

	engine.exit = make(chan struct{})
	<-engine.exit
	//TODO usubcribe
	//TODO waite for runing runers
	return nil
}

//Stop blocking
func (engine *Engine) Stop() {

	close(engine.exit)
}
