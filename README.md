# rabbitmqhandler

Work in progress

A RabbitMQ management library offering simple functions like easy queues and handlers declaration, message publishing and consumption.

How to use:

```
package main

import (
	"fmt"
	"service/queuehandler"
	"time"
)

func main() {
	q, err := queuehandler.NewQueueHandler("amqp://guest:guest@queue:5672/", 15)

	if err != nil {
		panic(err)
	}

	defer q.Close()

	q.DeclareQueue("test_q1")

	q.RegisterHandler("test_q1", func(res []byte) (err error) {
		fmt.Println("Message received on q1 service 1 !: " + string(res))

		return nil
	})

	err = q.Consume()

	if err != nil {
		panic(err)
	}

	go func() {
		time.Sleep(10 * time.Second)
		_ = q.Publish("test_q1", []byte("Hello from service 1 !"))
	}()

	forever := make(chan bool)

	<-forever
}
```
