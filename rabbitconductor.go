package rabbitconductor

import (
	"context"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

type QueueHandler struct {
	conn     *amqp091.Connection
	channel  *amqp091.Channel
	queues   map[string]amqp091.Queue
	handlers map[string]func(res []byte) (err error)
	running  bool
}

func NewQueueHandler(rabbitMQURL string, retries int) (q *QueueHandler, err error) {
	var conn *amqp091.Connection

	time.Sleep(10 * time.Second)
	// Queue connection ---------------------------------------------
	for i := 0; i < retries; i++ {
		conn, err = amqp091.Dial(rabbitMQURL)
		if err != nil {
			if i == retries-1 {
				return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
			}
			break
		} else {
			break
		}
		log.Printf("Failed to connect to RabbitMQ: %v", err)
		time.Sleep(time.Duration(2*i) * time.Second)
	}

	channel, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	return &QueueHandler{
		conn:     conn,
		channel:  channel,
		queues:   make(map[string]amqp091.Queue),
		handlers: make(map[string]func(res []byte) (err error)),
		running:  false,
	}, nil
}

func (q *QueueHandler) Close() {
	err := q.channel.Close()
	if err != nil {
		fmt.Println("Failed to close channel")
		return
	}
}

func (q *QueueHandler) DeclareQueue(name string) {
	if _, ok := q.queues[name]; ok {
		panic("queue %s already declared " + name)
	}

	newQueue, err := q.channel.QueueDeclare(
		name,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		panic(err)
	}

	q.queues[name] = newQueue
}

func (q *QueueHandler) Publish(queue string, message []byte) (err error) {
	if _, ok := q.queues[queue]; !ok {
		fmt.Println("Queue not declared")
		return
	}

	if !q.running {
		fmt.Println("Queue not running")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

	defer cancel()

	err = q.channel.PublishWithContext(
		ctx,
		"",
		queue,
		false,
		false,
		amqp091.Publishing{
			ContentType: "text/plain",
			Body:        message,
			Expiration:  "3600000",
		})

	if err != nil {
		return err
	}

	fmt.Println("Successfully published a message to the queue ! " + " " + queue)

	return nil
}

func (q *QueueHandler) RegisterHandler(queueName string, handler func(res []byte) (err error)) {
	if _, ok := q.queues[queueName]; !ok {
		panic("No queue declared " + queueName)
	}

	if _, ok := q.handlers[queueName]; ok {
		panic("Handler already registered for queue " + queueName)
	}

	if q.running {
		panic("Forbidden to register a handler while the queue is running")
	}

	q.handlers[queueName] = handler
}

func (q *QueueHandler) Consume() (err error) {
	for queueName, _ := range q.queues {
		msgs, err := q.channel.Consume(
			queueName, // queue
			"",        // consumer
			false,     // auto-ack
			false,     // exclusive
			false,     // no-local
			false,     // no-wait
			nil,       // args
		)

		if err != nil {
			log.Printf("%s", err)
		}

		queueName := queueName

		go func() {
			for d := range msgs {
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("Recovered from panic in RabbitMQ consumer: %v", r)
						}
					}()

					log.Printf("----> Worker received a message to process")

					err := q.handlers[queueName](d.Body)

					if err != nil {
						log.Printf("Failed to process the message: %s", err)
						// in case of error, we don't acknowledge the message, this could be set
						return
					}

					log.Printf("----> Worker finished to process the message")

					err = d.Ack(false)

					if err != nil {
						log.Printf("Failed to acknowledge the message: %s", err)
					} else {
						log.Println("Message acknowledged")
					}
				}()
			}
		}()
	}

	q.running = true

	return nil
}
