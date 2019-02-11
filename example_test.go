package pubsub_test

import (
	"fmt"

	"github.com/cskr/pubsub"
)

const topic = "topic"

func Example() {
	ps := pubsub.New(0)
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
	// Output:
	// Received message, 1 times.
	// Received message, 2 times.
	// Received message, 3 times.
	// Received message, 4 times.
	// Received message, 5 times.
	// Received message, 6 times.
}

func publish(ps *pubsub.PubSub) {
	for {
		ps.Pub("message", topic)
	}
}
