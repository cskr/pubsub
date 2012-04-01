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
// all of them receive any message published to the topic.
package pubsub

import "sync"

type any interface{}

// PubSub is a collection of topics.
type PubSub struct {
	topics map[string]map[chan any]bool
	mx     *sync.RWMutex
}

// New creates a new PubSub.
func New() *PubSub {
	topics := make(map[string]map[chan any]bool)
	mx := new(sync.RWMutex)
	return &PubSub{topics, mx}
}

// Sub returns a channel on which messages published to
// the specified topic can be received.
func (ps *PubSub) Sub(topic string) chan any {
	return ps.addSub(topic, false)
}

// SubOnce is similar to Sub, but only the first message
// published to the topic can be received.
func (ps *PubSub) SubOnce(topic string) chan any {
	return ps.addSub(topic, true)
}

// Pub publishes the given message to all subscribers of
// the specified topic.
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

// Unsub unsubscribed the given channels from the specified
// topic.
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
