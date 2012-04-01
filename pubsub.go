package pubsub

import "sync"

type any interface{}

type PubSub struct {
	topics map[string]map[chan any]bool
	mx     *sync.RWMutex
}

func New() *PubSub {
	topics := make(map[string]map[chan any]bool)
	mx := new(sync.RWMutex)
	return &PubSub{topics, mx}
}

func (ps *PubSub) Sub(topic string) chan any {
	return ps.addSub(topic, false)
}

func (ps *PubSub) SubOnce(topic string) chan any {
	return ps.addSub(topic, true)
}

func (ps *PubSub) Pub(topic string, msg any) {
	removes := make([]chan any, 0, 100)

	ps.mx.RLock()
	for ch, once := range ps.topics[topic] {
		ch <- msg
		if once {
			removes = append(removes, ch)
		}
	}
	ps.mx.RUnlock()

	if len(removes) > 0 {
		ps.Unsub(topic, removes...)
	}
}

func (ps *PubSub) Unsub(topic string, chans ...chan any) {
	ps.mx.Lock()
	for _, ch := range chans {
		delete(ps.topics[topic], ch)
	}

	if len(ps.topics[topic]) == 0 {
		delete(ps.topics, topic)
	}
	ps.mx.Unlock()
}

func (ps *PubSub) addSub(topic string, once bool) chan any {
	ch := make(chan any)

	ps.mx.Lock()
	if ps.topics[topic] == nil {
		ps.topics[topic] = make(map[chan any]bool)
	}

	ps.topics[topic][ch] = once
	ps.mx.Unlock()

	return ch
}
