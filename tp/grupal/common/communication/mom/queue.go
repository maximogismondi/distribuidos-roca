package mom

import (
	"fmt"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

// queue represents a RabbitMQ queue.
// It contains the channel to communicate with RabbitMQ, the queue name,
// and various properties of the queue.
// The queue name is the name used to identify the queue on the RabbitMQ server.
// The properties include whether the queue is durable, auto-deleted,
// exclusive, and whether to wait for the queue to be created.
// The args field can be used to pass additional arguments to the queue declaration.
type queue struct {
	channel     *amqp.Channel
	name        string
	amqpName    string
	durable     bool
	autoDeleted bool
	exclusive   bool
	noWait      bool
	args        amqp.Table
}

// newQueue creates a new RabbitMQ queue with the specified name.
// It initializes the queue properties and declares the queue on the RabbitMQ server.
// If the queue declaration fails, an error is logged and the program panics.
// The function returns a pointer to the newly created queue.
// The queue is durable, auto-deleted, and not exclusive by default.
// The noWait property is set to false, and no additional arguments are passed.
// The queue name is the name used to identify the queue on the RabbitMQ server.
func newQueue(ch *amqp.Channel, name string, args map[string]string) *queue {
	qArgs := amqp.Table{}

	if v, ok := args["expires"]; ok && v != "" {
		if val, err := strconv.Atoi(v); err == nil {
			log.Debugf("Setting expires for queue '%v': %v", name, v)
			qArgs["x-expires"] = val
		} else {
			log.Warningf("expires is not an int: %v", v)
		}
	}

	if v, ok := args["dlx_exchange"]; ok && v != "" {
		log.Debugf("Setting dead letter exchange for queue '%v': %v", name, v)
		qArgs["x-dead-letter-exchange"] = v
	}

	if v, ok := args["dlx_routingKey"]; ok && v != "" {
		log.Debugf("Setting dead letter routing key for queue '%v': %v", name, v)
		qArgs["x-dead-letter-routing-key"] = v
	}

	if v, ok := args["ttl"]; ok && v != "" {
		if val, err := strconv.Atoi(v); err == nil {
			log.Debugf("Setting message TTL for queue '%v': %v", name, v)
			qArgs["x-message-ttl"] = val
		} else {
			log.Warningf("ttl is not an int: %v", v)
		}
	}

	q := &queue{
		channel:     ch,
		name:        name,
		durable:     true,
		autoDeleted: false,
		exclusive:   false,
		noWait:      false,
		args:        qArgs,
	}

	amqpQ, err := ch.QueueDeclare(
		q.name,
		q.durable,
		q.autoDeleted,
		q.exclusive,
		q.noWait,
		qArgs,
	)

	q.amqpName = amqpQ.Name

	failOnError(err, fmt.Sprintf("Failed to declare the queue: '%v'", name))

	return q
}

// delete deletes the queue from the RabbitMQ server.
// It uses the channel's QueueDelete method to remove the queue.
// If the queue deletion fails, an error is logged and the program panics.
func (q *queue) delete() {
	_, err := q.channel.QueueDelete(
		q.amqpName,
		false,
		false,
		q.noWait,
	)

	failOnError(err, fmt.Sprintf("Failed to delete the queue: '%v'", q.amqpName))
}

// bind binds the queue to an exchange with the specified routing key.
func (q *queue) bind(exchangeName string, routingKey string) {
	err := q.channel.QueueBind(
		q.amqpName,
		routingKey,
		exchangeName,
		q.noWait,
		nil,
	)

	failOnError(err, fmt.Sprintf("Failed to bind the queue: '%v'", q.amqpName))
}

// unbind unbinds the queue from an exchange with the specified routing key.
func (q *queue) unbind(exchangeName string, routingKey string) {
	err := q.channel.QueueUnbind(
		q.amqpName,
		routingKey,
		exchangeName,
		nil,
	)
	failOnError(err, fmt.Sprintf("Failed to unbind the queue: '%v'", q.amqpName))
}

// consume registers a consumer for the queue.
// It returns a channel that receives messages from the queue.
// autoAck is set to false, meaning that the consumer must acknowledge the messages manually.
func (q *queue) consume() <-chan amqp.Delivery {
	msgs, err := q.channel.Consume(
		q.amqpName,
		q.amqpName,
		false,
		false,
		false,
		false,
		nil,
	)

	failOnError(err, fmt.Sprintf("Failed to register a consumer: '%v'", q.amqpName))

	q.channel.Qos(2, 0, false)

	return msgs
}

// close cancels the consumer for the queue.
// It does not close the channel, as it is assumed that the channel
// will be closed by the caller when it is no longer needed.
// If the cancellation fails, an error is logged.
// If the cancellation is successful, a message is logged indicating
// that the consumer was cancelled gracefully.
func (q *queue) close() {
	if err := q.channel.Cancel(q.amqpName, false); err != nil {
		log.Errorf("Failed to cancel the consumer '%v': '%v'", q.name, err)
	} else {
		log.Infof("Consumer '%v' cancelled gracefully ", q.name)
	}
}

// getHeadDelivery retrieves the head delivery from the queue.
// It returns the delivery and a boolean indicating whether the delivery was successful.
// If the queue does not exist, an error is logged and the function returns an empty delivery and false.
// The head delivery is the first message in the queue.
// The function uses the channel's Get method to retrieve the message.
// The autoAck parameter is set to false, meaning that the consumer must acknowledge the message manually.
func (q *queue) getHeadDelivery() (amqp.Delivery, bool) {
	msg, ok, err := q.channel.Get(q.amqpName, false)

	if err != nil {
		failOnError(err, fmt.Sprintf("Failed to get message from queue: '%v'", q.amqpName))
		return amqp.Delivery{}, false
	}

	return msg, ok
}
