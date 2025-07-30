package mom

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// exchangeContext is a wrapper around context.Context to manage the lifecycle of the exchange
// and its associated context.
// The context is used to set a timeout for operations on the exchange.
// The cancel function is used to release resources and stop operations when the exchange
// is no longer needed.
type exchangeContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// exchange represents a RabbitMQ exchange.
// It contains the channel to communicate with RabbitMQ, the exchange name,
// the exchange type, and various properties of the exchange.
// The exchange type can be "direct", "fanout", "topic", etc.
// The properties include whether the exchange is durable, auto-deleted,
// internal, and whether to wait for the exchange to be created.
// The args field can be used to pass additional arguments to the exchange declaration.
type exchange struct {
	channel     *amqp.Channel
	context     exchangeContext
	name        string
	kind        string
	durable     bool
	autoDeleted bool
	internal    bool
	noWait      bool
	args        []string
}

// newExchange creates a new RabbitMQ exchange with the specified name and type.
// It initializes the exchange properties and declares the exchange on the RabbitMQ server.
// The exchange is declared with a timeout of 5 seconds.
// If the exchange declaration fails, an error is logged and the program panics.
// The function returns a pointer to the newly created exchange.
// The exchange is durable, auto-deleted, and not internal by default.
// The noWait property is set to false, and no additional arguments are passed.
func newExchange(ch *amqp.Channel, name string, kind string) *exchange {
	ex := &exchange{
		channel:     ch,
		context:     exchangeContext{},
		name:        name,
		kind:        kind,
		durable:     true,
		autoDeleted: false,
		internal:    false,
		noWait:      false,
		args:        nil,
	}

	ex.context.ctx, ex.context.cancel = context.WithTimeout(context.Background(), 5*time.Second)

	err := ch.ExchangeDeclare(
		ex.name,
		ex.kind,
		ex.durable,
		ex.autoDeleted,
		ex.internal,
		ex.noWait,
		nil,
	)

	failOnError(err, fmt.Sprintf("Failed to declare the exchange: '%v'", name))

	return ex
}

// publish sends a message to the specified exchange with the given routing key.
// The message body is passed as a byte slice.
// The function uses the channel to publish the message to the exchange.
// The message is marked as persistent, and the content type is set to "text/plain".
// The message timestamp is set to the current time.
// If the publish operation fails, an error is logged and the program panics.
func (e *exchange) publish(routingKey string, body []byte) {
	err := e.channel.PublishWithContext(
		e.context.ctx,
		e.name,
		routingKey,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         body,
			Timestamp:    time.Now(),
		},
	)

	failOnError(err, fmt.Sprintf("Failed to publish a message to exchange '%v' with routing key '%v': %v", e.name, routingKey, err))
}

// close releases the resources associated with the exchange.
// It cancels the context, which stops any ongoing operations on the exchange.
// The function does not close the channel, as it is assumed that the channel
// will be closed by the caller when it is no longer needed.
func (e *exchange) close() {
	log.Infof("Closing exchange '%s'", e.name)
	e.context.cancel()
}
