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

	service1 := make(chan int)
	service2 := make(chan int)
	service3 := make(chan int)

	go fanIn(service1, service2, service3)

	service1 <- 1
	service2 <- 2
	service3 <- 3

	//go fanIn(service1,service2,service3)
	close(service1)
	close(service2)
	close(service3)
	time.Sleep(5 * time.Second)
}

func consume(c chan int) {
	for cn := range c {
		fmt.Println("writing....", cn)
	}
}

func fanIn(in ...chan int) {
	cons := make(chan int)

	go consume(cons)

	for cn := range in {
		cons <- cn
	}

	close(cons)

}

////pub sub
//
////deallok
//sync.waitGrup - implement /add / wait / done //baries
//
//call - Add
//
//caller wait
//
////2 chan - fan in
////access this methid 1 at time
//(t interface )
////done
//	case t : c <- "t"
//	case
//
//	N- pusblisher (, concurreny )  //buffer
//
//
////1 consumer
////fan in
//func (c ...chan)
//one_chan range list_of_chan
//subcriber  <- one_chan
//
//for () consurry {
//	last set of value
//}
//
//j range subcribe {
// 	fmt.Print(j)
//}
