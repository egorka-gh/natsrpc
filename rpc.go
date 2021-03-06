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
	sync.Mutex
	rpcs   map[string]*runer
	exit   chan struct{}
	closed bool
}

//Runer runs registered HandlerFunc on concrete subscription
type runer struct {
	sync.Mutex
	engine   *Engine
	topic    string
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
//subscribes on rpc command or adds handler(s) to existing subscription
func (engine *Engine) Register(cmd string, handler ...HandlerFunc) error {
	if engine.natsCnn == nil {
		return errors.New("natsrpc: has no connection")
	}
	if len(handler) == 0 {
		return errors.New("natsrpc: Handler required")
	}

	engine.Lock()
	defer engine.Unlock()

	if engine.closed {
		return errors.New("natsrpc: engine is stoped")
	}

	if engine.rpcs == nil {
		engine.rpcs = make(map[string]*runer)
	}
	subj := engine.topic(cmd)
	r, ok := engine.rpcs[subj]
	if !ok {
		//init rpc
		r = &runer{engine: engine, topic: subj, handlers: handlersChain{}}
		engine.rpcs[subj] = r
	}
	r.Lock()
	defer r.Unlock()

	log.Print("natsrpc: add handlers to " + subj)
	r.handlers = append(r.handlers, handler...)
	//subscribe if new rpc
	if !ok {
		sub, err := engine.natsCnn.Subscribe(subj, r.run)
		if err != nil {
			return err
		}
		r.sub = sub
		log.Print("natsrpc: subscribed to " + subj)
	}
	return nil
}

//IsRegistered - check if cmd is registered
func (engine *Engine) IsRegistered(cmd string) bool {
	if engine.natsCnn == nil {
		return false
	}
	engine.Lock()
	defer engine.Unlock()

	if engine.closed || engine.rpcs == nil {
		return false
	}

	_, ok := engine.rpcs[engine.topic(cmd)]
	return ok
}

//UnRegister - unsubscribe from rpc commad
//waits all running handlers
//TODO possible bug - while Unsubscribe nats can send message
func (engine *Engine) UnRegister(cmd string) error {
	if engine.natsCnn == nil {
		return errors.New("natsrpc: has no connection")
	}

	subj := engine.topic(cmd)

	engine.Lock()

	if engine.rpcs == nil || engine.closed {
		engine.Unlock()
		return nil
	}
	r, ok := engine.rpcs[subj]
	if !ok {
		engine.Unlock()
		return nil
	}

	r.Lock()
	if r.sub != nil {
		r.sub.Unsubscribe()
		r.sub = nil
	}
	delete(engine.rpcs, subj)
	log.Print("natsrpc: unsubscribed from " + subj)
	r.Unlock()
	engine.Unlock()

	r.wait()
	log.Print("natsrpc: complite UnRegister " + subj)
	return nil
}

//Run simple blocks current thread (waiting StopSignal)
func (engine *Engine) Run() error {

	if engine.natsCnn == nil {
		return errors.New("natsrpc: Not connected")
	}

	engine.Lock()
	if engine.closed {
		return errors.New("natsrpc: engine is stoped")
	}
	engine.exit = make(chan struct{})
	engine.Unlock()
	log.Print("natsrpc: Running")

	//loop while engine.exit not closed (StopSignal)
	for engine.exit != nil {
		select {
		case _, ok := <-engine.exit:
			if !ok {
				engine.Lock()
				engine.exit = nil
				engine.Unlock()
			}
		}
	}

	engine.Stop()

	return nil
}

//StopSignal stop blocking Run()
func (engine *Engine) StopSignal() {
	engine.Lock()
	defer engine.Unlock()
	if engine.exit != nil {
		close(engine.exit)
	}
}

//Stop stop engine
//unregister & stop all rpc
func (engine *Engine) Stop() error {
	//stop Run() if Stop() called directly
	defer engine.StopSignal()

	engine.Lock()
	defer engine.Unlock()
	if engine.closed {
		return nil
	}

	engine.closed = true

	log.Print("natsrpc: stopping...")

	//usubcribe
	for _, r := range engine.rpcs {
		if r.sub != nil {
			r.sub.Unsubscribe()
		}
	}

	//waite for all started handlers
	for _, r := range engine.rpcs {
		r.wait()
	}

	log.Print("natsrpc: stopped")
	return nil
}

//get command topic
func (engine *Engine) topic(cmd string) string {
	if len(engine.SubjectBase) > 0 {
		return engine.SubjectBase + "." + cmd
	}
	return cmd
}

//callback 4 nats msg
func (r *runer) run(msg *nats.Msg) {
	//TODO decode msg from msgp
	//create rpc message context
	m := &M{
		msg:    msg,
		engine: r.engine,
	}
	r.Lock()

	//copy(handlers, r.handlers)
	var handlers = append(handlersChain{}, r.handlers...)
	r.cnt++
	r.Unlock()

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
	m.engine.StopSignal()
}

//UnRegister rpc command
func (m *M) UnRegister(cmd string) error {
	return m.engine.UnRegister(cmd)
}

//IsRegistered - check if cmd is registered
func (m *M) IsRegistered(cmd string) bool {
	return m.engine.IsRegistered(cmd)
}
