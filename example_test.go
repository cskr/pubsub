package pubsub_test

import (
	"fmt"

	"github.com/cskr/pubsub/v2"
)

const topic = "topic"

func Example() {
	ps := pubsub.New[string, string](0)
	ch := ps.Sub(topic)
	go publish(ps)

	for i := 1; ; i++ {
		if i == 5 {
			// See the documentation of Unsub for why it is called in a new
			// goroutine.
			go ps.Unsub(ch, "topic")
		}

		if msg, ok := <-ch; ok {
			fmt.Printf("Received %s, %d times.\n", msg, i)
		} else {
			break
		}
	}
}

func publish(ps *pubsub.PubSub[string, string]) {
	for {
		ps.Pub("message", topic)
	}
}
