// Package Rabbitmqhandler is a wrapper around the RabbitMQ client. It provides a simple interface
// to declare queues and fanout exchanges, to publish messages, and to declare handlers like
// you would with an HTTP server.

package rabbitmqhandler

import (
	"context"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

type Handler func(msg []byte) (err error)

type RabbitMQHandler struct {
	conn      *amqp091.Connection
	channel   *amqp091.Channel
	queues    map[string]amqp091.Queue
	exchanges map[string]bool
	handlers  map[string]Handler
}

func New(rabbitMQURL string, retries uint8) (q *RabbitMQHandler, err error) {
	var conn *amqp091.Connection

	for i := uint8(0); i <= retries; i++ {
		conn, err = amqp091.Dial(rabbitMQURL)

		if err == nil {
			break
		}

		if err != nil && i == retries {
			return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
		}

		log.Printf("failed to connect to RabbitMQ, retrying in %d seconds", 2*(i+1))

		time.Sleep(time.Duration(2*(i+1)) * time.Second)
	}

	channel, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	return &RabbitMQHandler{
		conn:      conn,
		channel:   channel,
		queues:    make(map[string]amqp091.Queue),
		exchanges: make(map[string]bool),
		handlers:  make(map[string]Handler),
	}, nil
}

func (q *RabbitMQHandler) Close() (err error) {
	err = q.channel.Close()

	if err != nil {
		return err
	}

	return nil
}

func (q *RabbitMQHandler) DeclareQueue(name string) {
	if _, ok := q.queues[name]; ok {
		panic("queue %s is already declared " + name)
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

func (q *RabbitMQHandler) DeclareFanoutExchange(exchangeName string) {
	if _, ok := q.exchanges[exchangeName]; ok {
		panic("exchange %s is already declared " + exchangeName)
	}

	err := q.channel.ExchangeDeclare(
		exchangeName,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		panic(err)
	}

	q.exchanges[exchangeName] = true
}

func (q *RabbitMQHandler) BindQueueToExchange(queueName, exchangeName string) {
	if _, ok := q.queues[queueName]; !ok {
		panic("no queue declared " + queueName)
	}

	if _, ok := q.exchanges[exchangeName]; !ok {
		panic("no exchange declared " + exchangeName)
	}

	err := q.channel.QueueBind(
		queueName,
		"",
		exchangeName,
		false,
		nil,
	)

	if err != nil {
		panic(fmt.Errorf("failed to bind the queue %s to the exchange %s: %v",
			queueName, exchangeName, err))
	}
}

func (q *RabbitMQHandler) RegisterHandler(queueName string, handler func(res []byte) (err error)) {
	if _, ok := q.queues[queueName]; !ok {
		panic("no queue called " + queueName)
	}

	if _, ok := q.handlers[queueName]; ok {
		panic("no handler called " + queueName)
	}

	q.handlers[queueName] = handler
}

func (q *RabbitMQHandler) publish(exchange, key string, message []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

	defer cancel()

	err := q.channel.PublishWithContext(
		ctx,
		exchange,
		key,
		false,
		false,
		amqp091.Publishing{
			ContentType: "text/plain",
			Body:        message,
		},
	)

	if err != nil {
		return err
	}

	return nil
}

func (q *RabbitMQHandler) PublishToQueue(name string, message []byte) error {
	return q.publish("", name, message)
}

func (q *RabbitMQHandler) PublishToExchange(name, routingKey string, message []byte) error {
	return q.publish(name, routingKey, message)
}

func (q *RabbitMQHandler) Consume() (err error) {
	for queueName, _ := range q.handlers {
		msgs, err := q.channel.Consume(
			queueName,
			"",
			false,
			false,
			false,
			false,
			nil,
		)

		if err != nil {
			log.Printf("error consuming, %s", err)

			return err
		}

		queueName := queueName

		go func() {
			for msg := range msgs {
				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Printf("recovered from panic in rabbitmqhandler: %v", r)
						}
					}()

					err := q.handlers[queueName](msg.Body)

					if err != nil {
						log.Printf("failed to process the message: %s", err)

						return
					}

					err = msg.Ack(false)

					if err != nil {
						log.Printf("failed to acknowledge the message: %s", err)
					}
				}()
			}
		}()
	}

	return nil
}
