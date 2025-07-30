package mom

import (
	"strings"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

const URL = "amqp://guest:guest@rabbitmq:5672/"

type ConsumerChan <-chan amqp.Delivery

// failOnError checks if an error occurred and logs it.
// If an error occurred, it logs the error message and panics.
func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

// RabbitMQ represents a RabbitMQ connection and channel.
// It contains a map of queues and exchanges.
// The queues and exchanges are identified by their names.
// The queues and exchanges are created and managed by the RabbitMQ struct.
type RabbitMQ struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queues    map[string]*queue
	exchanges map[string]*exchange
}

// NewRabbitMQ creates a new RabbitMQ connection and channel.
func NewRabbitMQ() *RabbitMQ {
	conn, err := amqp.Dial(URL)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	return &RabbitMQ{
		conn:      conn,
		ch:        ch,
		queues:    make(map[string]*queue),
		exchanges: make(map[string]*exchange),
	}
}

// InitConfig initializes the RabbitMQ configuration with the specified exchanges and queues.
// It creates the exchanges and queues based on the provided configuration.
// The exchanges and queues are defined as slices of maps, where each map contains the name and type of the exchange or queue.
// The binds parameter specifies the binding between the queue and exchange.
// The routingKey is used as the routing key for the binding.
func (r *RabbitMQ) InitConfig(
	exchanges []map[string]string,
	queues []map[string]string,
	binds []map[string]string,
	routingKey string,
) {
	for _, exchange := range exchanges {
		r.NewExchange(exchange["name"], exchange["kind"])
	}

	for _, queue := range queues {
		r.NewQueue(queue["name"], map[string]string{
			"expires":        queue["expires"],
			"dlx_exchange":   queue["dlx_exchange"],
			"dlx_routingKey": queue["dlx_routingKey"],
			"ttl":            queue["ttl"],
		})
	}

	for _, bind := range binds {
		if rk, ok := bind["extraRK"]; ok {
			r.BindQueue(bind["queue"], bind["exchange"], rk)
		}

		if strings.Contains(bind["queue"], "eof") ||
			strings.Contains(bind["queue"], "control") {
			continue
		}

		r.BindQueue(bind["queue"], bind["exchange"], routingKey)
		r.BindQueue(bind["queue"], bind["exchange"], "")
	}
}

// NewQueue creates a new RabbitMQ queue with the specified name.
// If a queue with the same name already exists, an error is logged and the function returns.
func (r *RabbitMQ) NewQueue(name string, args map[string]string) {
	if _, ok := r.queues[name]; ok {
		log.Errorf("Queue '%s' already exists", name)
		return
	}

	r.queues[name] = newQueue(r.ch, name, args)
	log.Infof("Queue '%s' created", name)
}

// NewExchange creates a new RabbitMQ exchange with the specified name and type.
// The exchange type can be "direct", "fanout", "topic", etc.
// If an exchange with the same name already exists with a different type,
// an error is logged and the function returns.
func (r *RabbitMQ) NewExchange(name string, kind string) {
	if ex, ok := r.exchanges[name]; ok {
		if ex.kind != kind {
			log.Errorf("Exchange '%s' already exists with a different kind", name)
		}

		return
	}

	r.exchanges[name] = newExchange(r.ch, name, kind)
	log.Infof("Exchange '%s' of kind '%s' created", name, kind)
}

// Publish publishes a message to the specified exchange with the given routing key.
// The message body is passed as a byte slice.
// If the exchange does not exist, an error is logged and the function returns.
// The routing key is used to route the message to the appropriate queue(s).
func (r *RabbitMQ) Publish(exchangeName string, routingKey string, body []byte) {
	ex, ok := r.exchanges[exchangeName]

	if !ok {
		log.Errorf("Exchange '%s' does not exist", exchangeName)
		return
	}

	ex.publish(routingKey, body)
	log.Debugf("Message published to exchange '%s' with routing key '%s'", exchangeName, routingKey)
}

// Consume consumes messages from the specified queue.
// It returns a channel that receives messages from the queue.
// If the queue does not exist, an error is logged and the function returns nil.
func (r *RabbitMQ) Consume(queueName string) ConsumerChan {
	q, ok := r.queues[queueName]

	if !ok {
		log.Errorf("Queue '%s' does not exist", queueName)
		return nil
	}

	ret := q.consume()
	log.Infof("Consumer created for queue '%s'", queueName)

	return ret
}

// GetHeadDelivery retrieves the head delivery from the specified queue.
// It returns the delivery and a boolean indicating whether the delivery was successful.
// If the queue does not exist, an error is logged and the function returns an empty delivery and false.
// The head delivery is the first message in the queue.
// The autoAck parameter is set to false, meaning that the consumer must acknowledge the message manually.
func (r *RabbitMQ) GetHeadDelivery(queueName string) (amqp.Delivery, bool) {
	q, ok := r.queues[queueName]

	if !ok {
		log.Errorf("Queue '%s' does not exist", queueName)
		return amqp.Delivery{}, false
	}

	return q.getHeadDelivery()
}

// BindQueue binds the specified queue to the specified exchange with the given routing key.
// If the queue or exchange does not exist, an error is logged and the function returns.
// The routing key is used to route the messages from the exchange to the queue.
func (r *RabbitMQ) BindQueue(queueName string, exchangeName string, routingKey string) {
	q, ok := r.queues[queueName]

	if !ok {
		log.Errorf("Queue '%s' does not exist", queueName)
		return
	}

	q.bind(exchangeName, routingKey)
	log.Infof("Queue '%s' bound to exchange '%s' with routing key '%s'", queueName, exchangeName, routingKey)
}

// UnbindQueue unbinds the specified queue from the specified exchange with the given routing key.
// If the queue or exchange does not exist, an error is logged and the function returns.
// The routing key is used to unbind the messages from the exchange to the queue.
// This is useful when you want to stop receiving messages from the exchange with the specified routing key.
func (r *RabbitMQ) UnbindQueue(queueName string, exchangeName string, routingKey string) {
	q, ok := r.queues[queueName]

	if !ok {
		log.Errorf("Queue '%s' does not exist", queueName)
		return
	}

	q.unbind(exchangeName, routingKey)
	log.Infof("Queue '%s' unbound from exchange '%s' with routing key '%s'", queueName, exchangeName, routingKey)
}

// DeleteQueue deletes the specified queue.
// If the queue does not exist, an error is logged and the function returns.
// The queue is deleted from the RabbitMQ server and removed from the local map of queues.
// This removes all messages in the queue and binds to the queue.
// It is important to note that this operation is irreversible and should be used with caution.
func (r *RabbitMQ) DeleteQueue(queue string) {
	q, ok := r.queues[queue]

	if !ok {
		log.Errorf("Queue '%s' does not exist", queue)
		return
	}

	q.delete()
	log.Infof("Queue '%s' deleted", queue)
	delete(r.queues, queue)
}

// Close closes the RabbitMQ connection and channel.
// It also closes all the queues and exchanges.
// If an error occurs while closing the connection or channel, it is logged.
func (r *RabbitMQ) Close() {
	for _, queue := range r.queues {
		queue.close()
	}

	for _, exchange := range r.exchanges {
		exchange.close()
	}

	if err := r.ch.Close(); err != nil {
		log.Errorf("Failed to close the RabbitMQ channel: '%v'", err)
	}

	if err := r.conn.Close(); err != nil {
		log.Errorf("Failed to close the RabbitMQ connection: '%v'", err)
	}
}
