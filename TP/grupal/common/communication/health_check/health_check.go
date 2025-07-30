package health_check

import (
	"sync"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/mom"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

var log = logging.MustGetLogger("log")

type HealthCheck struct {
	rabbitMQ      *mom.RabbitMQ
	infraConfig   *model.InfraConfig
	pingChannel   mom.ConsumerChan
	running       bool
	containerName string
}

func NewHealthCheck(containerName string, infraConfig *model.InfraConfig) *HealthCheck {
	return &HealthCheck{
		rabbitMQ:      mom.NewRabbitMQ(),
		infraConfig:   infraConfig,
		containerName: containerName,
	}
}

func (hc *HealthCheck) InitConfig(exchanges, queues, binds []map[string]string, pingQueueName string) {
	hc.rabbitMQ.InitConfig(exchanges, queues, binds, hc.infraConfig.GetLeaderRK())

	hc.pingChannel = hc.rabbitMQ.Consume(
		pingQueueName,
	)

	hc.running = true
}

func (hc *HealthCheck) handlePing(msg *amqp.Delivery) {
	message := &protocol.HealthMessage{}
	err := proto.Unmarshal(msg.Body, message)
	if err != nil {
		log.Errorf("Failed to unmarshal message: %v", err)
		msg.Reject(false)
		return
	}

	switch message.GetMessage().(type) {
	case *protocol.HealthMessage_PingRequest:
		response := &protocol.HealthMessage{
			Message: &protocol.HealthMessage_PingResponse{
				PingResponse: &protocol.PingResponse{
					ContainerName: hc.containerName,
				},
			},
		}

		bytesMsg, err := proto.Marshal(response)
		if err != nil {
			log.Errorf("Error marshalling ping response: %v", err)
			msg.Reject(false)
			return
		}

		hc.rabbitMQ.Publish(
			hc.infraConfig.GetControlExchange(),
			hc.infraConfig.GetLeaderRK(),
			bytesMsg,
		)

		msg.Ack(false)
	default:
		log.Warningf("Received unexpected message type: %T", message.GetMessage())
		msg.Reject(false)
		return
	}
}

func (hc *HealthCheck) Run(wg *sync.WaitGroup) {
	defer wg.Done()
	defer hc.Stop()

	for hc.running {
		msg, ok := <-hc.pingChannel

		if !ok {
			log.Warning("Ping channel closed, stopping health check")
			hc.running = false
			return
		}

		hc.handlePing(&msg)
	}
}

func (hc *HealthCheck) Stop() {
	hc.running = false
	hc.rabbitMQ.Close()
}
