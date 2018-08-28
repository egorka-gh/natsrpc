package natsrpc

import (
	"errors"
	"log"
	"sync"

	"github.com/nats-io/go-nats"
)

//HandlerFunc is the standart callback to process rpc message
type HandlerFunc func(*M)

type handlersChain []HandlerFunc

/*Engine is the natsrpc instance
 */
type Engine struct {
	SubjectBase string
	Enc         Encoder

	natsCnn *nats.Conn
	rpcsMu  sync.RWMutex
	rpcs    map[string]*runer
	exit    chan struct{}
}

//Runer runs registered HandlerFunc on concrete subscription
type runer struct {
	mu       sync.Mutex
	engine   *Engine
	handlers handlersChain
	sub      *nats.Subscription
	cnt      int
	waiting  sync.WaitGroup
}

//M simple message context for HandlerFunc
type M struct {
	engine *Engine
	msg    *nats.Msg
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
		engine.rpcs = make(map[string]*runer)
	}
	subj := cmd
	if len(engine.SubjectBase) > 0 {
		subj = engine.SubjectBase + "." + subj
	}
	r, ok := engine.rpcs[subj]
	if !ok {
		//init rpc
		r = &runer{engine: engine, handlers: handlersChain{}}
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
				engine.rpcsMu.Lock()
				engine.exit = nil
				engine.rpcsMu.Unlock()
			}
		}
	}

	log.Print("natsrpc: Stopping...")

	//usubcribe
	for _, r := range engine.rpcs {
		if r.sub != nil {
			r.sub.Unsubscribe()
		}
	}

	//waite for started runers before quit
	for _, r := range engine.rpcs {
		r.wait()
	}
	log.Print("natsrpc: Exit")

	//exit
	return nil
}

//Stop blocking
func (engine *Engine) Stop() {
	engine.rpcsMu.Lock()
	defer engine.rpcsMu.Unlock()
	if engine.exit != nil {
		close(engine.exit)
	}
}

//callback 4 nats msg
func (r *runer) run(msg *nats.Msg) {
	//TODO decode msg from msgp
	//create copy
	m := &M{
		msg:    msg,
		engine: r.engine,
	}
	r.mu.Lock()
	//var handlers = make(handlersChain, 0, len(r.handlers))
	//copy(handlers, r.handlers)
	var handlers = append(handlersChain{}, r.handlers...)
	r.cnt++
	r.mu.Unlock()

	//run in parallel
	for _, h := range handlers {
		r.waiting.Add(1)
		go func(f HandlerFunc, m *M, wg *sync.WaitGroup) {
			defer wg.Done()
			f(m)
		}(h, m, &r.waiting)
	}
}

//waits all running goroutines
func (r *runer) wait() {
	r.waiting.Wait()
}

//Decode  decode msg body to obj using current encoder
func (m *M) Decode(obj interface{}) error {
	return m.engine.Enc.Decode(m.msg.Data, obj)
}

//Reply  encode msg body to obj using current encoder
// and  publish reply msg
func (m *M) Reply(obj interface{}) error {
	if m.msg.Reply == "" {
		return nil
	}
	//TODO encode msg.Data to msgp
	b, err := m.engine.Enc.Encode(obj)
	if err != nil {
		return err
	}
	return m.engine.natsCnn.Publish(m.msg.Reply, b)
}

//RawData  returns raw message data
func (m *M) RawData() []byte {
	if m.msg == nil {
		return nil
	}
	return m.msg.Data
}

//Subject returns rpc topic
func (m *M) Subject() string {
	if m.msg == nil {
		return ""
	}
	return m.msg.Subject
}

//ReplyTopic returns rpc reply topic
func (m *M) ReplyTopic() string {
	if m.msg == nil {
		return ""
	}
	return m.msg.Reply
}

//StopEngine stop engine
func (m *M) StopEngine() {
	m.engine.Stop()
}
