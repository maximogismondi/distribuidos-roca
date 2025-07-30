package listener

import (
	"sync"

	client_server_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/health_check"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/mom"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Listener struct {
	ch       *client_server_communication.ConnectionHandler
	ID       string
	rabbitMQ *mom.RabbitMQ

	healthCheck *health_check.HealthCheck
	wg          *sync.WaitGroup // WaitGroup to manage goroutines
}

func NewListener(id string, address string, containerName string, infraConfig *model.InfraConfig) *Listener {
	ch, err := client_server_communication.NewConnectionHandler(id, address)
	if err != nil {
		log.Panicf("Failed to parse RabbitMQ configuration: %s", err)
	}

	return &Listener{
		ch:          ch,
		ID:          id,
		rabbitMQ:    mom.NewRabbitMQ(),
		healthCheck: health_check.NewHealthCheck(containerName, infraConfig),
		wg:          &sync.WaitGroup{},
	}
}

func (l *Listener) InitConfig(exchanges []map[string]string, queues []map[string]string, binds []map[string]string) {
	l.rabbitMQ.InitConfig(exchanges, queues, binds, l.ID)
	l.healthCheck.InitConfig(
		exchanges,
		queues,
		binds,
		binds[0]["queue"],
	)
}

func (l *Listener) Run() error {
	go l.healthCheck.Run(l.wg)
	l.wg.Add(1)

	return l.ch.Run()
}

func (l *Listener) Stop() {
	l.healthCheck.Stop()
	log.Infof("Stopping listener with ID: %s", l.ID)
	l.ch.Stop()
	log.Infof("Listener with ID: %s stopped", l.ID)
	l.rabbitMQ.Close()
	l.wg.Wait()
}
