package health_checker

import (
	"os/exec"
	"time"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"google.golang.org/protobuf/proto"
)

func (hc *HealthChecker) runLeader() {
	defer hc.wg.Done()

	for hc.leaderID == hc.ID {
		hc.sendPing()

		time.Sleep(hc.healthCheckInterval)

		hc.readResponses()
		hc.sendStatus()
		hc.wakeUpContainers()
	}
}

func (hc *HealthChecker) wakeUpContainers() {
	for containerName, status := range hc.status {
		if status >= hc.maxStatus {
			cmd := exec.Command("docker", "restart", containerName)
			if err := cmd.Run(); err != nil {
				e, ok := err.(*exec.ExitError)
				if ok {
					log.Errorf("Failed to wake up container %s: %v; %v", containerName, e.String(), e.Stderr)
				} else {
					log.Errorf("Failed to wake up container %s: %v", containerName, err)
				}
			} else {
				log.Infof("Container %s is now awake", containerName)
			}
		}
	}
}

func (hc *HealthChecker) sendStatus() {
	msg := &protocol.HealthInternalMessage{
		Message: &protocol.HealthInternalMessage_Status{
			Status: &protocol.Status{
				Status: hc.status,
			},
		},
		SenderId: hc.ID,
	}

	bytesMsg, err := proto.Marshal(msg)
	if err != nil {
		log.Errorf("Error marshalling status message: %v", err)
		return
	}

	hc.rabbitMQ.Publish(
		hc.infraConfig.GetHealthExchange(),
		hc.infraConfig.GetBroadcastID(),
		bytesMsg,
	)

	log.Debugf("Status message: %v", hc.status)
	log.Debugf("Status message sent")
}

func (hc *HealthChecker) sendPing() {
	msg := &protocol.HealthMessage{
		Message: &protocol.HealthMessage_PingRequest{
			PingRequest: &protocol.PingRequest{},
		},
	}

	bytesMsg, err := proto.Marshal(msg)
	if err != nil {
		log.Errorf("Error marshalling ping request: %v", err)
		return
	}

	hc.rabbitMQ.Publish(
		hc.infraConfig.GetControlExchange(),
		hc.infraConfig.GetControlBroadcastRK(),
		bytesMsg,
	)

	log.Debugf("Ping request sent")
}

func (hc *HealthChecker) readResponses() {
	responses := make([]string, 0)

	for {
		msg, ok := hc.rabbitMQ.GetHeadDelivery(hc.leaderQueueName)

		if !ok {
			break
		}

		data := &protocol.HealthMessage{}

		err := proto.Unmarshal(msg.Body, data)
		if err != nil {
			log.Errorf("Error unmarshalling task: %v", err)
			continue
		}

		switch data.GetMessage().(type) {
		case *protocol.HealthMessage_PingResponse:
			pingResponse := data.GetPingResponse()
			responses = append(responses, pingResponse.GetContainerName())
		default:
			log.Warningf("Unknown or unexpected health message type: %T", data.GetMessage())
			msg.Reject(false)
			continue
		}

		msg.Ack(false)
	}

	for containerName := range hc.status {
		hc.status[containerName]++
	}

	for _, containerName := range responses {
		hc.status[containerName] = 0 // Reset status for containers that responded
	}

	log.Debugf("Responses read successfully")
	log.Debugf("Current status: %v", hc.status)
}
