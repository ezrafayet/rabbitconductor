# Rabbitmqhandler

Rabbitmqhandler is a wrapper around the RabbitMQ client. It provides a simple interface
to declare queues and fanout exchanges, to publish messages, and to declare handlers like 
you would with an HTTP server.

Notes:
- It is currently tailored for my own usage, so there is not much room for
configuration / multi channels usage and the patterns implemented are quite limited.
- It panics for obvious misconfiguration, and does not recover.

# Usage

## Quick start

### Install it
```
go get -u github.com/ezrafayet/rabbitmqhandler
```

### Initialize a new QueueHandler
```
q, err := rabbitmqhandler.New("amqp://guest:guest@queue:5672/", 15)
```

### Declare a queue
```
q.DeclareQueue("new_queue")
```

### Attach a handler to read from the queue
```
func handlerFunction (msg []byte) (err error) {
    fmt.Println("Message received: " + string(msg))

    return nil
}

q.RegisterHandler("new_queue", handlerFunction)
```

NB: to acknowledge the message, return nil from the handler.
To reject it, return an error.

### Publish to the queue
```
err = q.PublishToQueue("new_queue", []byte("Hello from service"))
```

## Fanout exchanges

Fanout exchanges let you broadcast messages to multiple queues.

### Declare a fanout exchange
```
// Create the fanout
q.DeclareFanoutExchange("new_fanout")

// Create the queue that will receive the messages
q.DeclareQueue("new_queue")

// Bind the queue to the fanout
q.BindQueueToExchange("new_queue", "new_fanout")
```

### Publish to the fanout exchange
```
err = q.PublishToExchange("new_fanout", []byte("Hello from service"))
```
