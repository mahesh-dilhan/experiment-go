package main

import (
	"fmt"
	"strconv"
	"time"
)

//Topic -> n Partitions
//Pubslicher ->push -> topic
//concumer -> n group
//consumer -> pull
func main() {
	done := make(chan bool)
	msgt1 := make(chan Message)
	//b := NewBrokerRecord(1, 1, 1, 1, &done)
	//pub := b.p
	//go pub.produce(*b.t, 5)
	//go b.con.c[0].consume

	b := NewBroker(1)
	en := NewEntry(b.bid)

	p := NewProducer(1000, &msgt1, &done)
	en.p = p

	cg := NewConsumerGroup(100)

	c := NewConsumer(5000)
	cg.c = []*Consumer{c}

	t := NewTopic(10000)

	par := NewPartiton(20000)
	t.p = []*Partition{par}

	msg1 := NewMessage(30000)
	msg1.msg = "Topic 1 service 1...."

	msg2 := NewMessage(30001)
	msg2.msg = "Topic 2 service 2...."

	par.msgs = []*Message{msg1, msg2}

	time.Sleep(5 * time.Second)
}

type Message struct {
	msg string
	mid int
}

func NewMessage(mid int) *Message {
	return &Message{mid: mid}
}

type Partition struct {
	msgs   []*Message
	parid  int
	offset int
}

func NewPartiton(parid int) *Partition {
	return &Partition{
		parid: parid,
	}
}

type Topic struct {
	p   []*Partition
	tid int
}

func NewTopic(tid int) *Topic {
	return &Topic{tid: tid}
}

type Entry struct {
	con *ConsumerGroup
	p   *Producer
	t   *Topic
	eid int
}

func NewEntry(eid int) *Entry {
	return &Entry{
		eid: eid,
	}
}

func NewBroker(bid int) *Broker {
	return &Broker{
		e:   []*Entry{},
		bid: bid,
	}
}

type Broker struct {
	e   []*Entry
	bid int
}

func NewConsumerGroup(cgid int) *ConsumerGroup {
	return nil
}

type ConsumerGroup struct {
	c    []*Consumer
	cgid int
}

func (cg *ConsumerGroup) joinGroup(newc *Consumer) {
	cg.c = append(cg.c, newc)
}
func NewConsumer(cid int) *Consumer {
	return &Consumer{
		cid: cid,
	}
}

type Consumer struct {
	cid  int
	msgs *chan Message
}

func subscribe(b int, topic Topic) *int {
	return nil
}

type Producer struct {
	id   int
	msgs *chan Message
	done *chan bool
}

func NewProducer(id int, msgs *chan Message, done *chan bool) *Producer {
	return &Producer{id: id, msgs: msgs, done: done}
}

//func NewBrokerRecord(pid int, cid int, tid int, parid int, done *chan bool) *Broker {
//	cn := make(chan Message)
//
//	return &Broker{
//		p: &Producer{
//			id:   pid,
//			done: done,
//			msgs: nil,
//		},
//		con: &ConsumerGroup{
//			c: []*Consumer{
//				{id: cid, msgs: &cn},
//			},
//		},
//		t: &Topic{
//			tid: tid,
//			p: []*Partition{
//				{id: parid, msgs: nil},
//			},
//		},
//	}
//}

//func (b *Broker) connectpusblish(pid int,done *chan bool) *Broker {
//
//
//}

func (p *Producer) produce(topic Topic, max int) {
	fmt.Println("produce: Started")
	var m *Message
	for i := 0; i < max; i++ {
		fmt.Println("produce: Sending ", i)
		m = &Message{
			msg:    "service" + strconv.Itoa(i),
			offset: i,
		}
		fmt.Println(*m)
		*p.msgs <- *m
	}
	*p.done <- true // signal when done
	fmt.Println("produce: Done")
}

func (c *Consumer) consume() {
	fmt.Println("consume: Started")
	for {
		msg := <-*c.msgs
		fmt.Println("consume: Received:", msg)
	}
}
