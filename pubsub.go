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
// Topics must be strings and messages of any type can be
// published. A topic can have any number of subcribers and
// all of them receive messages published on the topic.
package pubsub

// PubSub is a collection of topics.
type PubSub struct {
	sub, subOnce, pub, unsub chan cmd
	unsubAll                 chan chan interface{}
	close                    chan string
	shutdown                 chan bool
	capacity                 int
}

type cmd struct {
	topic string
	data  interface{}
}

// New creates a new PubSub and starts a goroutine for handling operations.
// The capacity of the channels created by Sub and SubOnce will be as specified.
func New(capacity int) *PubSub {
	ps := new(PubSub)
	ps.capacity = capacity

	ps.sub = make(chan cmd)
	ps.subOnce = make(chan cmd)
	ps.pub = make(chan cmd)
	ps.unsub = make(chan cmd)
	ps.unsubAll = make(chan chan interface{})
	ps.close = make(chan string)
	ps.shutdown = make(chan bool)

	go ps.start()

	return ps
}

// Sub returns a channel on which messages published on any of
// the specified topics can be received.
func (ps *PubSub) Sub(topics ...string) chan interface{} {
	return ps.doSub(ps.sub, topics...)
}

// SubOnce is similar to Sub, but only the first message published, after subscription,
// on any of the specified topics can be received.
func (ps *PubSub) SubOnce(topics ...string) chan interface{} {
	return ps.doSub(ps.subOnce, topics...)
}

// AddSub adds subscriptions to an existing channel.
func (ps *PubSub) AddSub(ch chan interface{}, topics ...string) {
	for _, topic := range topics {
		ps.sub <- cmd{topic, ch}
	}
}

func (ps *PubSub) doSub(cmdChan chan cmd, topics ...string) chan interface{} {
	ch := make(chan interface{}, ps.capacity)
	for _, topic := range topics {
		cmdChan <- cmd{topic, ch}
	}
	return ch
}

// Pub publishes the given message to all subscribers of
// the specified topics.
func (ps *PubSub) Pub(msg interface{}, topics ...string) {
	for _, topic := range topics {
		ps.pub <- cmd{topic, msg}
	}
}

// Unsub unsubscribes the given channel from the specified
// topics. If no topic is specified, it is unsubscribed
// from all topics.
func (ps *PubSub) Unsub(ch chan interface{}, topics ...string) {
	if len(topics) == 0 {
		ps.unsubAll <- ch
		return
	}

	for _, topic := range topics {
		ps.unsub <- cmd{topic, ch}
	}
}

// Close closes all channels currently subscribed to the specified topics.
// If a channel is subscribed to multiple topics, some of which is
// not specified, it is not closed.
func (ps *PubSub) Close(topics ...string) {
	for _, topic := range topics {
		ps.close <- topic
	}
}

// Shutdown closes all subscribed channels and terminates the goroutine.
func (ps *PubSub) Shutdown() {
	ps.shutdown <- true
}

// registry maintains the current subscription state. It's not
// safe to access a registry from multiple goroutines simultaneously.
type registry struct {
	topics    map[string]map[chan interface{}]bool
	revTopics map[chan interface{}]map[string]bool
}

func (ps *PubSub) start() {
	reg := registry{
		topics:    make(map[string]map[chan interface{}]bool),
		revTopics: make(map[chan interface{}]map[string]bool),
	}

loop:
	for {
		select {
		case cmd := <-ps.sub:
			reg.add(cmd.topic, cmd.data.(chan interface{}), false)

		case cmd := <-ps.subOnce:
			reg.add(cmd.topic, cmd.data.(chan interface{}), true)

		case cmd := <-ps.pub:
			reg.send(cmd.topic, cmd.data.(interface{}))

		case cmd := <-ps.unsub:
			reg.remove(cmd.topic, cmd.data.(chan interface{}))

		case ch := <-ps.unsubAll:
			reg.removeChannel(ch)

		case topic := <-ps.close:
			reg.removeTopic(topic)

		case <-ps.shutdown:
			break loop
		}
	}

	for topic, chans := range reg.topics {
		for ch, _ := range chans {
			reg.remove(topic, ch)
		}
	}
}

func (reg *registry) add(topic string, ch chan interface{}, once bool) {
	if reg.topics[topic] == nil {
		reg.topics[topic] = make(map[chan interface{}]bool)
	}
	reg.topics[topic][ch] = once

	if reg.revTopics[ch] == nil {
		reg.revTopics[ch] = make(map[string]bool)
	}
	reg.revTopics[ch][topic] = true
}

func (reg *registry) send(topic string, msg interface{}) {
	for ch, once := range reg.topics[topic] {
		ch <- msg
		if once {
			for topic := range reg.revTopics[ch] {
				reg.remove(topic, ch)
			}
		}
	}
}

func (reg *registry) removeTopic(topic string) {
	for ch := range reg.topics[topic] {
		reg.remove(topic, ch)
	}
}

func (reg *registry) removeChannel(ch chan interface{}) {
	for topic := range reg.revTopics[ch] {
		reg.remove(topic, ch)
	}
}

func (reg *registry) remove(topic string, ch chan interface{}) {
	if _, ok := reg.topics[topic]; !ok {
		return
	}

	if _, ok := reg.topics[topic][ch]; !ok {
		return
	}

	delete(reg.topics[topic], ch)
	delete(reg.revTopics[ch], topic)

	if len(reg.topics[topic]) == 0 {
		delete(reg.topics, topic)
	}

	if len(reg.revTopics[ch]) == 0 {
		close(ch)
		delete(reg.revTopics, ch)
	}
}
