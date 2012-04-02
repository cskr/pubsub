/*
 * Copyright (C) 2012 Chandra Sekar S
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package pubsub implements a simple multi-topic pub-sub
// library.
//
// Topic names must be strings and messages of any type
// can be published. A topic can have any number of subcribers and
// all of them receive the messages published to the topic.
package pubsub

type any interface{}

// PubSub is a collection of topics.
type PubSub struct {
	topics                   map[string]map[chan any]bool
	sub, subOnce, pub, unsub chan cmd
}

type cmd struct {
	topic string
	data  any
}

// New creates a new PubSub.
func New() *PubSub {
	topics := make(map[string]map[chan any]bool)
	ps := PubSub{topics, make(chan cmd), make(chan cmd), make(chan cmd), make(chan cmd)}
	go ps.start()
	return &ps
}

// Sub returns a channel on which messages published to
// the specified topic can be received.
func (ps *PubSub) Sub(topic string) chan any {
	ch := make(chan any)
	ps.sub <- cmd{topic, ch}
	return ch
}

// SubOnce is similar to Sub, but only the first message
// published to the topic can be received.
func (ps *PubSub) SubOnce(topic string) chan any {
	ch := make(chan any)
	ps.subOnce <- cmd{topic, ch}
	return ch
}

// Pub publishes the given message to all subscribers of
// the specified topic.
func (ps *PubSub) Pub(topic string, msg any) {
	ps.pub <- cmd{topic, msg}
}

// Unsub unsubscribes the given channel from the specified
// topic.
func (ps *PubSub) Unsub(topic string, ch chan any) {
	ps.unsub <- cmd{topic, ch}
}

func (ps *PubSub) start() {
	for {
		select {
		case cmd := <-ps.sub:
			ps.add(cmd.topic, cmd.data.(chan any), false)

		case cmd := <-ps.subOnce:
			ps.add(cmd.topic, cmd.data.(chan any), true)

		case cmd := <-ps.pub:
			ps.send(cmd.topic, cmd.data.(any))

		case cmd := <-ps.unsub:
			ps.remove(cmd.topic, cmd.data.(chan any))
		}
	}
}

func (ps *PubSub) add(topic string, ch chan any, once bool) {
	if ps.topics[topic] == nil {
		ps.topics[topic] = make(map[chan any]bool)
	}

	ps.topics[topic][ch] = once
}

func (ps *PubSub) send(topic string, msg any) {
	for ch, once := range ps.topics[topic] {
		ch <- msg
		if once {
			ps.remove(topic, ch)
		}
	}
}

func (ps *PubSub) remove(topic string, ch chan any) {
	close(ch)
	delete(ps.topics[topic], ch)
	if len(ps.topics[topic]) == 0 {
		delete(ps.topics, topic)
	}
}
