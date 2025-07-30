package worker

import (
	"os"
	"sync"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/health_check"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/mom"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/actions"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

var log = logging.MustGetLogger("log")

// Worker is a struct that represents a worker that consumes tasks from a message queue and executes them
type Worker struct {
	WorkerId    string
	baseDir     string
	rabbitMQ    *mom.RabbitMQ
	action      actions.Action
	done        chan os.Signal
	consumeChan mom.ConsumerChan
	eofChan     mom.ConsumerChan

	healthCheck *health_check.HealthCheck
	wg          *sync.WaitGroup
}

// NewWorker creates a new worker with the given id, type, and infraConfig
// and initializes RabbitMQ and action structs
// It also takes a signal channel to handle SIGTERM signal
func NewWorker(workerType string, infraConfig *model.InfraConfig, signalChan chan os.Signal, containerName string) *Worker {
	rabbitMQ := mom.NewRabbitMQ()

	action := actions.NewAction(workerType, infraConfig)

	log.Infof("[Worker] Llamando a action.LoadData para tipo %s en directorio %s", workerType, infraConfig.GetDirectory())
	err := action.LoadData(infraConfig.GetDirectory())
	if err != nil {
		log.Panicf("Failed to load data: %s", err)
		return nil
	}
	log.Infof("[Worker] action.LoadData terminó para tipo %s. Action: %T", workerType, action)

	return &Worker{
		WorkerId:    infraConfig.GetNodeId(),
		baseDir:     infraConfig.GetDirectory(),
		rabbitMQ:    rabbitMQ,
		action:      action,
		done:        signalChan,
		consumeChan: nil,
		healthCheck: health_check.NewHealthCheck(containerName, infraConfig),
		wg:          &sync.WaitGroup{},
	}

}

// InitConfig initializes the worker with the given exchanges, queues, and binds
// It expects to load one bind from the config file
// If the number of binds is not equal to 1, it panics
// It also initializes the RabbitMQ with the given exchanges, queues, and binds
// and sets the consume channel to the queue specified in the bind
// The workerId is used as routingKey for the bind
func (w *Worker) InitConfig(exchanges []map[string]string, queues []map[string]string, binds []map[string]string) {
	w.rabbitMQ.InitConfig(exchanges, queues, binds, w.WorkerId)
	w.consumeChan = w.rabbitMQ.Consume(binds[0]["queue"])
	w.eofChan = w.rabbitMQ.Consume(binds[1]["queue"])

	w.healthCheck.InitConfig(
		exchanges,
		queues,
		binds,
		binds[2]["queue"],
	)
}

// Run starts the worker and listens for messages on the consume channel
// EOF messages are processed with priority over regular task messages
func (w *Worker) Run() {
	if w.consumeChan == nil || w.eofChan == nil {
		log.Panicf("Consume channel is nil, did you call InitConfig?")
	}

	defer w.Shutdown()

	go w.healthCheck.Run(w.wg)
	w.wg.Add(1)

outer:
	for {
		select {
		case <-w.done:
			log.Infof("Worker %s received SIGTERM", w.WorkerId)
			break outer
		case message, ok := <-w.eofChan:
			if !ok {
				log.Warningf("Worker %s eof consume channel closed", w.WorkerId)
				break outer
			}
			w.handleMessage(&message)
		case message, ok := <-w.consumeChan:
			if !ok {
				log.Warningf("Worker %s consume channel closed", w.WorkerId)
				break outer
			}
			w.handleMessage(&message)
		}
	}

	log.Infof("Worker stop running gracefully")
}

// handleMessage handles the incoming message from the RabbitMQ
// It unmarshals the task from the message body and executes it using the action struct
// It also sends the subTasks to the RabbitMQ for each exchange and routing key
// If the task fails to unmarshal or execute, it logs the error and continues to the next message
// It also acknowledges the message after processing it
func (w *Worker) handleMessage(message *amqp.Delivery) {
	taskRaw := message.Body

	task := &protocol.Task{}

	err := proto.Unmarshal(taskRaw, task)

	if err != nil {
		log.Errorf("Failed to unmarshal task: %s", err)
		message.Reject(false)
		return
	}

	log.Debugf("Task %T recived with a TID of %s",
		task.GetStage(), task.GetTaskIdentifier())
	subTasks, err := w.action.Execute(task)

	if err != nil {
		log.Errorf("Failed to execute task: %s", err)
		message.Reject(false)
		return
	}
	log.Debugf("Task %T executed with a TID of %s",
		task.GetStage(), task.GetTaskIdentifier())

	w.sendSubTasks(subTasks)
	// w.sendSubTasks(subTasks)

	err = w.action.SaveData(task)
	if err != nil {
		log.Errorf("Failed to save data: %s", err)
		message.Reject(false)
		return
	}

	log.Debugf("[worker] Task %T with TID %s saved successfully",
		task.GetStage(), task.GetTaskIdentifier())

	message.Ack(false)

	log.Debugf("[worker] Task %T with TID %s acknowledged",
		task.GetStage(), task.GetTaskIdentifier())

	err = w.action.DeleteData(task)
	if err != nil {
		log.Errorf("Failed to delete data: %s", err)
		message.Reject(false)
		return
	}
	log.Debugf("[worker] Task %T with TID %s deleted successfully",
		task.GetStage(), task.GetTaskIdentifier())

}

// sendSubTasks sends the subTasks to the RabbitMQ for each exchange and routing key
// It marshals the task to a byte array and publishes it to the RabbitMQ
// It logs the task, exchange, and routing key for debugging purposes
// This function is nil-safe, meaning it will not panic if the input is nil
// It will simply return without doing anything
func (w *Worker) sendSubTasks(subTasks common.Tasks) {
	sendTask := func(exchange, routingKey string, task *protocol.Task) {
		taskRaw, err := proto.Marshal(task)

		if err != nil {
			log.Errorf("Failed to marshal task: %s", err)
			return
		}

		w.rabbitMQ.Publish(exchange, routingKey, taskRaw)
		log.Debugf("Task %T sent to exchange '%s' with routing key '%s' with a TID of %s",
			task.GetStage(), exchange, routingKey, task.GetTaskIdentifier())
	}

	for exchange, routingKeys := range subTasks {
		for routingKey, tasks := range routingKeys {
			for _, task := range tasks {
				sendTask(exchange, routingKey, task)
			}
		}
	}
}

// Shutdown closes the RabbitMQ connection
// It is safe to call this method multiple times
func (w *Worker) Shutdown() {
	w.healthCheck.Stop()
	w.rabbitMQ.Close()
	w.wg.Wait()
}
