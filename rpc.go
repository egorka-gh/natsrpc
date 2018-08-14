package natsrpc

import (
	"errors"
	"log"
	"sync"

	"github.com/nats-io/go-nats"
)

//HandlerFunc is the standart callback to process rpc message
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

//Runer runs registered HandlerFunc on concrete subscription
type Runer struct {
	mu       sync.Mutex
	engine   *Engine
	handlers handlersChain
	sub      *nats.Subscription
	msg      *nats.Msg
}

/*New ctreats Engine
* cnn - nats connection
* base - subject prefix for rpc commands
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
//rpc commad is the last part of subj, if SubjectBase='bigzz.api' and cmd='getbonus' full subject = 'bigzz.api.getbonus'
func (engine *Engine) Register(cmd string, handler ...HandlerFunc) error {
	if engine.natsCnn == nil {
		return errors.New("natsrpc: has no connection")
	}
	if len(handler) == 0 {
		return errors.New("natsrpc: Handler required")
	}

	engine.rpcsMu.Lock()
	defer engine.rpcsMu.Unlock()

	if engine.rpcs == nil {
		engine.rpcs = make(map[string]*Runer)
	}
	subj := engine.SubjectBase + "." + cmd
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
		return errors.New("natsrpc: Not connected")
	}

	log.Print("natsrpc: Started")

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

//RawData  returns raw message data
func (r *Runer) RawData() []byte {
	if r.msg == nil {
		return nil
	}
	return r.msg.Data
}

//Subject returns rpc topic
func (r *Runer) Subject() string {
	if r.msg == nil {
		return ""
	}
	return r.msg.Subject
}

//ReplyTo returns rpc reply topic
func (r *Runer) ReplyTo() string {
	if r.msg == nil {
		return ""
	}
	return r.msg.Reply
}

//StopEngine stop engine
func (r *Runer) StopEngine() {
	r.engine.Stop()
}
