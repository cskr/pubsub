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

// internalSubscriptions is a collection of topics that exist only in the threads closed context
type internalSubscriptions struct {
	topics                   map[string]map[chan interface{}]bool
	revTopics                map[chan interface{}]map[string]bool
}

// PubSub is a collection of channels to interface with PubSub start thread.
type PubSub struct {
	sub, subOnce, pub, unsub, unsubAll chan cmd
	close                              chan string
	shutdown                           chan bool
	capacity                           int
}

type cmd struct {
	topic string
	data  interface{}
}

// New creates a new PubSub and starts a goroutine for handling operations.
// The capacity of the channels created by Sub and SubOnce will be as specified.
func newInternalSubscription() *internalSubscriptions {
	is := new(internalSubscriptions)
	is.topics = make(map[string]map[chan interface{}]bool)
	is.revTopics = make(map[chan interface{}]map[string]bool)

	return is
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
	ps.unsubAll = make(chan cmd)
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

// SubUpdate adds subscriptions to an existing channel.
func (ps *PubSub) SubUpdate(ch chan interface{}, topics ...string) {
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
// topics.
func (ps *PubSub) Unsub(ch chan interface{}, topics ...string) {
	for _, topic := range topics {
		ps.unsub <- cmd{topic, ch}
	}
}

// UnsubAll unsubscribes the given channel from all topics
func (ps *PubSub) UnsubAll(ch chan interface{}) {
    ps.unsubAll <- cmd{"", ch}
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

func (ps *PubSub) start() {
    is := newInternalSubscription()

loop:
	for {
		select {
		case cmd := <-ps.sub:
			is.add(cmd.topic, cmd.data.(chan interface{}), false)

		case cmd := <-ps.subOnce:
			is.add(cmd.topic, cmd.data.(chan interface{}), true)

		case cmd := <-ps.pub:
			is.send(cmd.topic, cmd.data.(interface{}))

		case cmd := <-ps.unsub:
			is.remove(cmd.topic, cmd.data.(chan interface{}))

		case cmd := <-ps.unsubAll:
			is.removeChannel(cmd.data.(chan interface{}))

		case topic := <-ps.close:
			is.removeTopic(topic)

		case <-ps.shutdown:
			break loop
		}
	}

	for topic, chans := range is.topics {
		for ch, _ := range chans {
			is.remove(topic, ch)
		}
	}
}

func (is *internalSubscriptions) add(topic string, ch chan interface{}, once bool) {
	if is.topics[topic] == nil {
		is.topics[topic] = make(map[chan interface{}]bool)
	}
	is.topics[topic][ch] = once

	if is.revTopics[ch] == nil {
		is.revTopics[ch] = make(map[string]bool)
	}
	is.revTopics[ch][topic] = true
}

func (is *internalSubscriptions) send(topic string, msg interface{}) {
	for ch, once := range is.topics[topic] {
		ch <- msg
		if once {
			for topic := range is.revTopics[ch] {
				is.remove(topic, ch)
			}
		}
	}
}

func (is *internalSubscriptions) removeTopic(topic string) {
	for ch := range is.topics[topic] {
		is.remove(topic, ch)
	}
}

func (is *internalSubscriptions) removeChannel(ch chan interface{}) {
    for topic, _ := range is.revTopics[ch] {
        is.remove(topic, ch)
    }
}

func (is *internalSubscriptions) remove(topic string, ch chan interface{}) {
	if _, ok := is.topics[topic]; !ok {
		return
	}

	if _, ok := is.topics[topic][ch]; !ok {
		return
	}

	delete(is.topics[topic], ch)
	delete(is.revTopics[ch], topic)

	if len(is.topics[topic]) == 0 {
		delete(is.topics, topic)
	}

	if len(is.revTopics[ch]) == 0 {
		close(ch)
		delete(is.revTopics, ch)
	}
}
