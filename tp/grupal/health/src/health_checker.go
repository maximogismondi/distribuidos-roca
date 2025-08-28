package health_checker

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/health_check"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/mom"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

var log = logging.MustGetLogger("log")

type HealthStatus = map[string]uint32

type HealthChecker struct {
	rabbitMQ            *mom.RabbitMQ
	ID                  string
	healthCheckInterval time.Duration
	infraConfig         *model.InfraConfig
	leaderQueueName     string
	status              HealthStatus
	maxStatus           uint32
	healthChannel       mom.ConsumerChan
	done                chan os.Signal
	leaderID            string
	wg                  *sync.WaitGroup // WaitGroup to manage goroutines

	electionTimeout       time.Duration    // Duration for election timeout
	electionTimeoutC      <-chan time.Time // Channel to signal election timeout
	leaderTimeoutC        <-chan time.Time // Channel to signal leader timeout
	electionTimeoutActive bool
	leaderTimeoutActive   bool

	healthCheck *health_check.HealthCheck
}

// Hardcode expected nodes for health checker.
// Otherwise if a new node that is not expected send a ping to the leader, it is
// appended to the status map.
// This function is in case when a node is down before it can answer a ping, so the leader
// will not receive a status update from it.
func initStatus(ic *model.InfraConfig) HealthStatus {
	status := make(HealthStatus)

	for i := range ic.GetFilterCount() {
		key := fmt.Sprintf("filter_%v", i)
		status[key] = 0 // Initialize all containers with status 0
	}

	for i := range ic.GetMapCount() {
		key := fmt.Sprintf("mapper_%v", i)
		status[key] = 0 // Initialize all containers with status 0
	}

	for i := range ic.GetOverviewCount() {
		key := fmt.Sprintf("overviewer_%v", i)
		status[key] = 0 // Initialize all containers with status 0
	}

	for i := range ic.GetJoinCount() {
		key := fmt.Sprintf("joiner_%v", i)
		status[key] = 0 // Initialize all containers with status 0
	}

	for i := range ic.GetReduceCount() {
		key := fmt.Sprintf("reducer_%v", i)
		status[key] = 0 // Initialize all containers with status 0
	}

	for i := range ic.GetMergeCount() {
		key := fmt.Sprintf("merger_%v", i)
		status[key] = 0 // Initialize all containers with status 0
	}

	for i := range ic.GetTopCount() {
		key := fmt.Sprintf("topper_%v", i)
		status[key] = 0 // Initialize all containers with status 0
	}

	for i := range ic.GetHealthCount() {
		key := fmt.Sprintf("health_%v", i)
		status[key] = 0 // Initialize all containers with status 0
	}

	for i := range ic.GetServerCount() {
		key := fmt.Sprintf("server_%v", i)
		status[key] = 0 // Initialize all containers with status 0
	}

	log.Infof("Initialized health status: %v", status)

	return status

}

func NewHealthChecker(
	id string,
	healthCheckInterval int,
	infraConfig *model.InfraConfig,
	leaderQueueName string,
	maxStatus uint32,
	signalChan chan os.Signal,
	electionTimeout int,
	containerName string,
) *HealthChecker {
	randomDuration := time.Duration(rand.Float32()) * time.Second

	return &HealthChecker{
		rabbitMQ:            mom.NewRabbitMQ(),
		ID:                  id,
		healthCheckInterval: time.Duration(healthCheckInterval) * time.Millisecond,
		infraConfig:         infraConfig,
		leaderQueueName:     leaderQueueName,
		status:              initStatus(infraConfig),
		maxStatus:           maxStatus,
		electionTimeoutC:    nil,
		leaderTimeoutC:      nil,
		done:                signalChan,
		wg:                  &sync.WaitGroup{},
		electionTimeout:     time.Duration(electionTimeout)*time.Millisecond + randomDuration,
		healthCheck:         health_check.NewHealthCheck(containerName, infraConfig),
	}
}

func (hc *HealthChecker) InitConfig(exchanges, queues, binds []map[string]string) {
	hc.rabbitMQ.InitConfig(exchanges, queues, binds, hc.ID)
	hc.healthCheck.InitConfig(exchanges, queues, binds, binds[2]["queue"])
	hc.healthChannel = hc.rabbitMQ.Consume(binds[1]["queue"])
}

func (hc *HealthChecker) resetElectionTimeout() {
	// Cancel previous timeout channel by creating a new one
	// and updating the active flag
	hc.stopElectionTimeout()
	hc.electionTimeoutC = time.After(hc.electionTimeout)
	hc.electionTimeoutActive = true
}

func (hc *HealthChecker) stopElectionTimeout() {
	// We can't actually stop the channel, but we can mark it as inactive
	// so we know to ignore it in the select statement
	hc.electionTimeoutActive = false
	// Replace the channel with nil to make it clear it's not active
	hc.electionTimeoutC = nil
}

func (hc *HealthChecker) resetLeaderTimeout() {
	hc.stopLeaderTimeout()
	hc.leaderTimeoutC = time.After(hc.electionTimeout)
	hc.leaderTimeoutActive = true
}

func (hc *HealthChecker) stopLeaderTimeout() {
	hc.leaderTimeoutActive = false
	hc.leaderTimeoutC = nil
}

func (hc *HealthChecker) sendLeader() {
	// Logic to send a leader message to the leader queue
	log.Info("I'm the leader, sending leader message")
	hc.stopLeaderTimeout()
	hc.stopElectionTimeout()

	msg := &protocol.HealthInternalMessage{
		Message: &protocol.HealthInternalMessage_Leader{
			Leader: &protocol.Leader{
				LeaderId: hc.ID,
			},
		},
		SenderId: hc.ID,
	}

	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		log.Errorf("Failed to marshal leader message: %v", err)
		return
	}

	hc.rabbitMQ.Publish(
		hc.infraConfig.GetHealthExchange(),
		hc.infraConfig.GetBroadcastID(),
		msgBytes,
	)

	if hc.leaderID != hc.ID {
		hc.leaderID = hc.ID
		go hc.runLeader()
		hc.wg.Add(1)
	}

	log.Debugf("Leader message sent: %s", hc.ID)
}

func (hc *HealthChecker) sendElection() {
	// Logic to send an election message to the leader queue
	log.Debug("Sending election message")

	hc.stopLeaderTimeout()
	hc.stopElectionTimeout()

	msg := &protocol.HealthInternalMessage{
		Message: &protocol.HealthInternalMessage_Election{
			Election: &protocol.Election{
				HealtcheckerId: hc.ID,
			},
		},
		SenderId: hc.ID,
	}

	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		log.Errorf("Failed to marshal election message: %v", err)
		return
	}

	hc.rabbitMQ.Publish(
		hc.infraConfig.GetHealthExchange(),
		hc.infraConfig.GetBroadcastID(),
		msgBytes,
	)

	// Start leader timeout - if no higher ID node responds within this timeout,
	// this node will declare itself the leader
	hc.resetLeaderTimeout()

	log.Debugf("Election message sent: %s", hc.ID)
}

func (hc *HealthChecker) Run() {
	defer hc.Stop()

	go hc.healthCheck.Run(hc.wg)
	hc.wg.Add(1)

	// Start with election timeout to detect when to initiate an election
	// hc.resetElectionTimeout()
	hc.sendElection()

outer:
	for {
		select {
		case <-hc.done:
			log.Infof("HealthChecker %s received SIGTERM", hc.ID)
			hc.leaderID = ""
			break outer

		case message, ok := <-hc.healthChannel:
			if !ok {
				log.Warningf("HealthChecker %s internal health consume channel closed", hc.ID)
				break outer
			}
			hc.handleMessage(&message)

		case <-hc.electionTimeoutC:
			if hc.electionTimeoutActive {
				log.Infof("Election timeout reached for HealthChecker %s, sending election", hc.ID)
				// Haven't heard from a leader in a while, start an election
				hc.sendElection()
			}

		case <-hc.leaderTimeoutC:
			if hc.leaderTimeoutActive {
				log.Infof("Leader timeout reached for HealthChecker %s, sending leader message", hc.ID)
				// No higher ID node responded to my election message within the timeout,
				// so I should become the leader
				hc.sendLeader()
			}
		}
	}
}

func (hc *HealthChecker) handleMessage(message *amqp.Delivery) {
	internalMsg := &protocol.HealthInternalMessage{}
	if err := proto.Unmarshal(message.Body, internalMsg); err != nil {
		log.Errorf("Failed to unmarshal health internal message: %v", err)
		return
	}

	if hc.ID == internalMsg.GetSenderId() {
		log.Debugf("Ignoring message from self: %s of type %T", hc.ID, internalMsg.Message)
		message.Reject(false)
		return
	}

	switch msg := internalMsg.Message.(type) {
	case *protocol.HealthInternalMessage_Election:
		// If I'm already the leader, respond with a leader message instead of election
		if hc.ID > internalMsg.GetElection().GetHealtcheckerId() {
			if hc.leaderID == hc.ID {
				log.Debugf("Received election message while I'm the leader, sending leader message")
				hc.sendLeader() // Remind everyone I'm the leader
			} else {
				hc.sendElection()
				log.Debugf("Sent election message, my id is higher: %s", hc.ID)
			}
		} else {
			// If my ID is lower, I should back off and let the higher ID node lead
			hc.stopLeaderTimeout() // Stop any pending leader timeout
			hc.leaderID = ""
			hc.resetElectionTimeout()
			log.Debugf("Received election message from %s, but my id is lower", internalMsg.GetSenderId())
		}

	case *protocol.HealthInternalMessage_Status:
		// When receiving a status update from the leader, we know the leader is alive
		hc.stopLeaderTimeout() // Stop any pending leader timeout

		// Only reset election timeout if I'm not the leader
		if hc.leaderID != hc.ID {
			hc.resetElectionTimeout() // Reset election timeout - the leader is active
		}
		statusMsg := internalMsg.GetStatus()
		hc.status = statusMsg.GetStatus()
		log.Debugf("Received status message from leader %s", hc.leaderID)

	case *protocol.HealthInternalMessage_Leader:
		// When a leader is declared, all nodes should recognize it
		hc.stopLeaderTimeout() // Stop any pending leader timeout
		leaderMsg := internalMsg.GetLeader()
		hc.leaderID = leaderMsg.GetLeaderId()

		if hc.leaderID != hc.ID {
			hc.resetElectionTimeout() // Reset election timeout - the leader is active
		}

		log.Debugf("Received leader message from %s", hc.leaderID)

	default:
		log.Warningf("Unknown message type: %T", msg)
		message.Reject(false)
		return
	}

	message.Ack(false)
}

func (hc *HealthChecker) Stop() {
	hc.stopElectionTimeout()
	hc.stopLeaderTimeout()
	hc.healthCheck.Stop()
	hc.wg.Wait()
	hc.rabbitMQ.Close()
}
