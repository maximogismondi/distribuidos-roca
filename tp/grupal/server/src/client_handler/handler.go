package client_handler

import (
	"errors"
	"fmt"

	client_server_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/rabbit"
	"github.com/google/uuid"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")
var ErrSignalReceived = errors.New("signal received")

type ClientHandler struct {
	ServerID      string
	socket        *client_server_communication.Socket
	ClientID      string
	isRunning     bool
	rabbitHandler *rabbit.RabbitHandler
}

func NewClienthandler(serverID string, infraConfig *model.InfraConfig, socketFd uintptr) (*ClientHandler, error) {
	socket, err := client_server_communication.NewClientSocketFromFile(socketFd)

	if err != nil {
		return nil, err
	}

	return &ClientHandler{
		socket:        socket,
		isRunning:     true,
		rabbitHandler: rabbit.NewRabbitHandler(infraConfig),
		ServerID:      serverID,
	}, nil
}

// InitConfig initializes the server with the given exchanges, queues, and binds
func (c *ClientHandler) InitConfig(exchanges []map[string]string, queues []map[string]string, binds []map[string]string) {
	c.rabbitHandler.InitConfig(c.ServerID, exchanges, queues, binds)
}

func (c *ClientHandler) Run() error {
	defer c.socket.Close()

	if err := c.handleConnection(); err != nil {
		if !c.isRunning {
			log.Infof("action: handleConnection | result: fail | error: %v", ErrSignalReceived)
		} else if err == client_server_communication.ErrConnectionClosed {
			log.Infof("action: handleConnection | result: fail | error: %v | clientId: %s", err, c.ClientID)
		} else {
			log.Errorf("action: handleConnection | result: fail | error: %v", err)
		}
		return err
	}

	return nil
}

func (c *ClientHandler) handleMessage(message *protocol.Message) error {
	clientServerMessage, ok := message.GetMessage().(*protocol.Message_ClientServerMessage)

	if !ok {
		return fmt.Errorf("unexpected message type: expected ServerClientMessage")
	}

	switch msg := clientServerMessage.ClientServerMessage.GetMessage().(type) {
	case *protocol.ClientServerMessage_Sync:
		c.handleConnectionMessage(msg.Sync)

	case *protocol.ClientServerMessage_Batch:
		c.handleBatchMessage(msg.Batch)

	case *protocol.ClientServerMessage_FileEof:
		c.handleFileEOFMessage(msg.FileEof)

	case *protocol.ClientServerMessage_Finish:
		c.handleFinishMessage(msg.Finish)

	case *protocol.ClientServerMessage_Result:
		c.handleResultMessage(msg.Result)

	case *protocol.ClientServerMessage_Disconnect:
		c.handleDisconnectMessage(msg.Disconnect)

	default:
		// Handle unknown message type
	}
	return nil
}

func (c *ClientHandler) handleConnection() error {
	for c.isRunning {
		message, err := c.socket.Read()
		if err != nil {
			return err
		}

		err = c.handleMessage(message)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *ClientHandler) handleConnectionMessage(syncMessage *protocol.Sync) {
	var clientID string = syncMessage.GetClientId()

	if len(clientID) == 0 {
		clientID = generateUniqueID()
	}

	idMessage := &protocol.Message{
		Message: &protocol.Message_ServerClientMessage{
			ServerClientMessage: &protocol.ServerClientMessage{
				Message: &protocol.ServerClientMessage_SyncAck{
					SyncAck: &protocol.SyncAck{
						ClientId: clientID,
					},
				},
			},
		},
	}

	c.ClientID = clientID
	log.Infof("Client connected with ID: %s", clientID)
	c.rabbitHandler.RegisterNewClient(clientID)

	if err := c.socket.Write(idMessage); err != nil {
		log.Errorf("Error sending ID to client: %v", err)
		return
	}
}

func (c *ClientHandler) handleBatchMessage(batchMessage *protocol.Batch) error {
	clientId := batchMessage.GetClientId()
	taskNumber := batchMessage.GetBatchNumber()

	switch batchMessage.GetType() {
	case protocol.FileType_MOVIES:
		movies := processMoviesBatch(batchMessage)
		c.rabbitHandler.SendMoviesRabbit(movies, clientId, taskNumber)
	case protocol.FileType_CREDITS:
		actors := processCreditsBatch(batchMessage)
		c.rabbitHandler.SendActorsRabbit(actors, clientId, taskNumber)
	case protocol.FileType_RATINGS:
		ratings := processRatingsBatch(batchMessage)
		c.rabbitHandler.SendRatingsRabbit(ratings, clientId, taskNumber)
	default:
		log.Errorf("Invalid batch type: %v", batchMessage.Type)

		ackMessage := &protocol.Message{
			Message: &protocol.Message_ServerClientMessage{
				ServerClientMessage: &protocol.ServerClientMessage{
					Message: &protocol.ServerClientMessage_BatchAck{
						BatchAck: &protocol.BatchAck{
							Status: protocol.MessageStatus_FAIL,
						},
					},
				},
			},
		}

		if err := c.socket.Write(ackMessage); err != nil {
			log.Errorf("Error sending batch acknowledgment: %v", err)
			return err
		}

		return fmt.Errorf("invalid batch type: %v", batchMessage.Type)
	}

	log.Debugf("Received batch message with type: %v", batchMessage.Type)

	ackMessage := &protocol.Message{
		Message: &protocol.Message_ServerClientMessage{
			ServerClientMessage: &protocol.ServerClientMessage{
				Message: &protocol.ServerClientMessage_BatchAck{
					BatchAck: &protocol.BatchAck{
						Status: protocol.MessageStatus_SUCCESS,
					},
				},
			},
		},
	}

	if err := c.socket.Write(ackMessage); err != nil {
		log.Errorf("Error sending batch acknowledgment: %v", err)
		return err
	}

	return nil
}

func (c *ClientHandler) handleFileEOFMessage(fileEOFMessage *protocol.FileEOF) error {
	log.Infof("Received file EOF message from user: %v ", fileEOFMessage.ClientId)

	clientId := fileEOFMessage.GetClientId()
	taskCount := fileEOFMessage.GetBatchCount()

	switch fileEOFMessage.GetType() {
	case protocol.FileType_MOVIES:
		c.rabbitHandler.SendMoviesEOFRabbit(clientId, taskCount)
	case protocol.FileType_CREDITS:
		c.rabbitHandler.SendActorsEOFRabbit(fileEOFMessage.ClientId, taskCount)
	case protocol.FileType_RATINGS:
		c.rabbitHandler.SendRatingsEOFRabbit(fileEOFMessage.ClientId, taskCount)
	default:
		log.Errorf("Invalid file type in EOF message: %v", fileEOFMessage.GetType())

		ackMessage := &protocol.Message{
			Message: &protocol.Message_ServerClientMessage{
				ServerClientMessage: &protocol.ServerClientMessage{
					Message: &protocol.ServerClientMessage_FileEofAck{
						FileEofAck: &protocol.FileEOFAck{
							Status: protocol.MessageStatus_FAIL,
						},
					},
				},
			},
		}

		if err := c.socket.Write(ackMessage); err != nil {
			log.Errorf("Error sending file EOF acknowledgment: %v", err)
			return err
		}

		return fmt.Errorf("invalid file type in EOF message: %v", fileEOFMessage.GetType())
	}

	ackMessage := &protocol.Message{
		Message: &protocol.Message_ServerClientMessage{
			ServerClientMessage: &protocol.ServerClientMessage{
				Message: &protocol.ServerClientMessage_FileEofAck{
					FileEofAck: &protocol.FileEOFAck{
						Status: protocol.MessageStatus_SUCCESS,
					},
				},
			},
		},
	}

	if err := c.socket.Write(ackMessage); err != nil {
		log.Errorf("Error sending file EOF acknowledgment: %v", err)
		return err
	}

	return nil
}

func (c *ClientHandler) handleFinishMessage(finishMessage *protocol.Finish) {
	log.Infof("Received finish message from user: %v ", finishMessage.ClientId)

	c.rabbitHandler.RemoveClient(finishMessage.ClientId)
	log.Infof("Queue removed for client ID: %s", finishMessage.ClientId)

	ackMessage := &protocol.Message{
		Message: &protocol.Message_ServerClientMessage{
			ServerClientMessage: &protocol.ServerClientMessage{
				Message: &protocol.ServerClientMessage_FinishAck{
					FinishAck: &protocol.FinishAck{},
				},
			},
		},
	}

	if err := c.socket.Write(ackMessage); err != nil {
		log.Errorf("Error sending finish acknowledgment: %v", err)
		return
	}

	// Client finished, exit this child process gracefully
	c.Stop()
}

func (c *ClientHandler) handleDisconnectMessage(disconnectMessage *protocol.Disconnect) {
	log.Infof("Received disconnect message from user: %v ", disconnectMessage.ClientId)

	ackMessage := &protocol.Message{
		Message: &protocol.Message_ServerClientMessage{
			ServerClientMessage: &protocol.ServerClientMessage{
				Message: &protocol.ServerClientMessage_DisconnectAck{
					DisconnectAck: &protocol.DisconnectAck{},
				},
			},
		},
	}

	if err := c.socket.Write(ackMessage); err != nil {
		log.Errorf("Error sending disconnect acknowledgment: %v", err)
		return
	}

	// Client disconnected, exit this child process gracefully
	c.Stop()
}

func (c *ClientHandler) handleResultMessage(resultMessage *protocol.Result) error {
	log.Infof("Received result message from user: %v ", resultMessage.ClientId)

	results, msg := c.rabbitHandler.GetResults(resultMessage.ClientId)

	message := &protocol.Message{
		Message: &protocol.Message_ServerClientMessage{
			ServerClientMessage: &protocol.ServerClientMessage{
				Message: &protocol.ServerClientMessage_Results{
					Results: results,
				},
			},
		},
	}

	log.Debugf("Sending results to client %v, status: %v", resultMessage.ClientId, results.GetStatus())

	if err := c.socket.Write(message); err != nil {
		log.Errorf("Error sending results: %v", err)
		return err
	}

	c.rabbitHandler.AckResults(msg)

	return nil
}

func generateUniqueID() string {
	return uuid.NewString()
}

func (c *ClientHandler) Stop() {
	c.isRunning = false

	if c.socket != nil {
		c.socket.Close()
		log.Infof("Client socket closed")
	}

	if c.rabbitHandler != nil {
		c.rabbitHandler.Close()
		log.Infof("RabbitMQ connection closed")
	}
}
