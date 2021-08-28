package main

import (
	"fmt"
	"time"
)

//Topic -> n Partitions
//Pubslicher ->push -> topic
//concumer -> n group
//consumer -> pull

func main() {
	done := make(chan bool)
	msgt1 := make(chan Message)

	msg1 := NewMessage(30000)
	msg1.msg = "Topic 1 service 1...."

	msg2 := NewMessage(30001)
	msg2.msg = "Topic 2 service 2...."

	par := NewPartition(20000)
	//par.msgs = []*Message{msg1, msg2}

	t := NewTopic(10000, "Covid")
	t.p = []*Partition{par}

	fmt.Printf(" Partitions '%v' \n", *par)
	fmt.Printf(" Topic '%v \n", *t)

	b := NewBroker(1)
	en := NewEntry(b.bid)
	en.t = t
	en.done = &done
	en.msgs = &msgt1

	p := NewProducer(1000)
	en.p = p

	cg := NewConsumerGroup(100)
	en.con = cg
	c := NewConsumer(5000)
	cg.joinGroup(c)

	fmt.Printf("Consumer '%v' \n", *c)
	fmt.Printf("Consumer Group '%v' \n", *cg)
	fmt.Printf("Producer '%v' \n", *p)
	fmt.Printf("Broker '%v' \n", *b)
	fmt.Printf("Entry '%v' \n", *en)

	var msgarr = []*Message{msg1, msg2}
	for _, m := range msgarr {
		go en.produce(t, m)
	}

	for _, m := range msgarr {
		go en.produce(t, m)
	}

	go en.consume()

	<-done
	time.Sleep(5 * time.Second)
}

type Message struct {
	msg string
	mid int //offset
}

func NewMessage(mid int) *Message {
	return &Message{mid: mid}
}

type Partition struct {
	msgs  []*Message
	parid int
}

func (par *Partition) pushToPartition(msg *Message) {
	par.msgs = append(par.msgs, msg)
}

func NewPartition(parid int) *Partition {
	return &Partition{
		parid: parid,
	}
}

type Topic struct {
	p    []*Partition
	tid  int
	name string
}

func (t *Topic) getPartition() *Partition {
	//default 0
	return t.p[0]
}

func NewTopic(tid int, name string) *Topic {
	return &Topic{tid: tid, name: name}
}

type Entry struct {
	con  *ConsumerGroup
	p    *Producer
	t    *Topic
	eid  int
	msgs *chan Message
	done *chan bool
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
	return &ConsumerGroup{
		c:    []*Consumer{},
		cgid: cgid,
	}
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
	cid int
	//msgs *chan Message
}

type Producer struct {
	id int
}

func NewProducer(id int) *Producer {
	return &Producer{id: id}
}

func (e *Entry) produce(topic *Topic, msg ...*Message) {
	fmt.Println("produce: Started")

	for _, m := range msg {
		fmt.Println("produce: Sending ", *m)
		topic.getPartition().pushToPartition(m)
		*e.msgs <- *m
	}

	*e.done <- true // signal when done
	fmt.Println("produce: Done")
}

func (e *Entry) consume() {
	fmt.Println("consume: Started")
	for {
		msg := <-*e.msgs
		fmt.Println("consume: Received:", msg)
	}
}
