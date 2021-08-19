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
	//var done = make(chan bool)
	//b := NewBrokerRecord(1, 1, 1, 1, &done)
	//pub := b.p
	//go pub.produce(*b.t, 5)
	//go b.con.c[0].consume()

	time.Sleep(5 * time.Second)
}

type Message struct {
	msg    string
	offset int
}
type Partition struct {
	msgs []*Message
	id   int
}
type Topic struct {
	p   []*Partition
	tid int
}

type Entry struct {
	con    *ConsumerGroup
	p      *Producer
	t      *Topic
	record int
}

func subscriber(b int, topic Topic) *int {
	return nil
}

func NewEntry() *Entry {
	return &Entry{}
}

type Broker struct {
	e []*Entry
}

type ConsumerGroup struct {
	c []*Consumer
}
type Consumer struct {
	id   int
	msgs *chan Message
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
