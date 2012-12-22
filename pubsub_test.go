package pubsub

import (
	check "launchpad.net/gocheck"
	"testing"
)

var _ = check.Suite(new(Suite))

func Test(t *testing.T) {
	check.TestingT(t)
}

type Suite struct{}

func (s *Suite) TestSub(c *check.C) {
	ps := New(1)
	ch1, err := ps.Sub("t1")
	c.Check(err, check.Equals, nil)
	ch2, err := ps.Sub("t1")
	c.Check(err, check.Equals, nil)
	ch3, err := ps.Sub("t2")
	c.Check(err, check.Equals, nil)

	err = ps.Pub("t1", "hi")
	c.Check(err, check.Equals, nil)
	c.Check(<-ch1, check.Equals, "hi")
	c.Check(<-ch2, check.Equals, "hi")

	err = ps.Pub("t2", "hello")
	c.Check(<-ch3, check.Equals, "hello")

	ps.Shutdown()
	_, ok := <-ch1
	c.Check(ok, check.Equals, false)
	_, ok = <-ch2
	c.Check(ok, check.Equals, false)
	_, ok = <-ch3
	c.Check(ok, check.Equals, false)
}

func (s *Suite) TestSubOnce(c *check.C) {
	ps := New(1)
	ch, err := ps.SubOnce("t1")
	c.Check(err, check.Equals, nil)

	err = ps.Pub("t1", "hi")
	c.Check(err, check.Equals, nil)
	c.Check(<-ch, check.Equals, "hi")

	_, ok := <-ch
	c.Check(ok, check.Equals, false)
	ps.Shutdown()
}

func (s *Suite) TestUnsub(c *check.C) {
	ps := New(1)
	ch, err := ps.Sub("t1")
	c.Check(err, check.Equals, nil)

	err = ps.Pub("t1", "hi")
	c.Check(err, check.Equals, nil)
	c.Check(<-ch, check.Equals, "hi")

	ps.Unsub("t1", ch)
	_, ok := <-ch
	c.Check(ok, check.Equals, false)
	ps.Shutdown()
}
