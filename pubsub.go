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
// all of them receive the messages published to the topic.
package pubsub

import "errors"

// PubSub is a collection of topics.
type PubSub struct {
	topics                   map[string]map[chan interface{}]bool
	sub, subOnce, pub, unsub chan cmd
	shutdown                 bool
}

type cmd struct {
	topic string
	data  interface{}
}

// New creates a new PubSub and starts a goroutine for handling operations.
func New() *PubSub {
	topics := make(map[string]map[chan interface{}]bool)
	ps := PubSub{topics, make(chan cmd), make(chan cmd), make(chan cmd), make(chan cmd), false}
	go ps.start()
	return &ps
}

// Sub returns a channel on which messages published to
// the specified topic can be received. Sub returns err != nil
// if PubSub was shutdown.
func (ps *PubSub) Sub(topic string) (ch chan interface{}, err error) {
	return ps.dosub(ps.sub, topic)
}

// SubOnce is similar to Sub, but only the first message
// published to the topic can be received.
func (ps *PubSub) SubOnce(topic string) (ch chan interface{}, err error) {
	return ps.dosub(ps.subOnce, topic)
}

func (ps *PubSub) dosub(cmdChan chan cmd, topic string) (ch chan interface{}, err error) {
	if ps.shutdown {
		err = errors.New("Sub after Shutdown")
		return
	}

	ch = make(chan interface{})
	cmdChan <- cmd{topic, ch}
	return
}

// Pub publishes the given message to all subscribers of
// the specified topic. Pub returns an error if PubSub
// was shutdown.
func (ps *PubSub) Pub(topic string, msg interface{}) error {
	if ps.shutdown {
		return errors.New("Pub after Shutdown")
	}

	ps.pub <- cmd{topic, msg}
	return nil
}

// Unsub unsubscribes the given channel from the specified
// topic. Unsub returns an error if PubSub was shutdown.
func (ps *PubSub) Unsub(topic string, ch chan interface{}) error {
	if ps.shutdown {
		return errors.New("Unsub after Shutdown")
	}

	ps.unsub <- cmd{topic, ch}
	return nil
}

// Shutdown closes all subscribed channels. PubSub cannot be used
// after it has been shutdown.
func (ps *PubSub) Shutdown() {
	if ps.shutdown {
		return
	}

	ps.shutdown = true

	close(ps.sub)
	close(ps.subOnce)
	close(ps.pub)
	close(ps.unsub)
}

func (ps *PubSub) start() {
loop:
	for {
		select {
		case cmd, ok := <-ps.sub:
			if !ok {
				break loop
			}
			ps.add(cmd.topic, cmd.data.(chan interface{}), false)

		case cmd, ok := <-ps.subOnce:
			if !ok {
				break loop
			}
			ps.add(cmd.topic, cmd.data.(chan interface{}), true)

		case cmd, ok := <-ps.pub:
			if !ok {
				break loop
			}
			ps.send(cmd.topic, cmd.data.(interface{}))

		case cmd, ok := <-ps.unsub:
			if !ok {
				break loop
			}
			ps.remove(cmd.topic, cmd.data.(chan interface{}))
		}
	}

	for topic, chans := range ps.topics {
		for ch, _ := range chans {
			ps.remove(topic, ch)
		}
	}
}

func (ps *PubSub) add(topic string, ch chan interface{}, once bool) {
	if ps.topics[topic] == nil {
		ps.topics[topic] = make(map[chan interface{}]bool)
	}

	ps.topics[topic][ch] = once
}

func (ps *PubSub) send(topic string, msg interface{}) {
	for ch, once := range ps.topics[topic] {
		ch <- msg
		if once {
			ps.remove(topic, ch)
		}
	}
}

func (ps *PubSub) remove(topic string, ch chan interface{}) {
	close(ch)
	delete(ps.topics[topic], ch)
	if len(ps.topics[topic]) == 0 {
		delete(ps.topics, topic)
	}
}
