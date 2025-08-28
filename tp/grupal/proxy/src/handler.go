package proxy

import (
	"errors"
	"strconv"

	client_server_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/op/go-logging"
)

var ErrSignalReceived = errors.New("signal received")
var ErrNoServersAvailable = errors.New("no servers available")
var log = logging.MustGetLogger("log")

type Handler struct {
	clientSocket  *client_server_communication.Socket
	serverSocket  *client_server_communication.Socket
	isRunning     bool
	serverAddress []string
	currentIndex  int
}

func NewHandler(serverAddresses []string, addressIndex string, socketFd uintptr) (*Handler, error) {
	clientSocket, err := client_server_communication.NewClientSocketFromFile(socketFd)
	if err != nil {
		return nil, err
	}

	index, err := strconv.Atoi(addressIndex)
	if err != nil {
		return nil, errors.New("invalid addressIndex: must be an integer")
	}

	handler := &Handler{
		clientSocket:  clientSocket,
		serverSocket:  nil,
		isRunning:     true,
		serverAddress: serverAddresses,
		currentIndex:  index,
	}

	handler.serverSocket, err = handler.selectServer()
	if err != nil {
		log.Errorf("action: selectServer | result: fail | error: %v", err)
		return nil, err
	}

	return handler, nil
}

func (h *Handler) selectServer() (*client_server_communication.Socket, error) {
	if len(h.serverAddress) == 0 {
		log.Panic("action: selectServer | result: fail | error: no servers available")
	}

	var connected bool = false
	var serverSocket *client_server_communication.Socket
	var err error

	startIndex := h.currentIndex

	for !connected {
		serverAddress := h.serverAddress[h.currentIndex]
		log.Infof("action: selectServer | server: %s", serverAddress)

		serverSocket, err = client_server_communication.Connect(serverAddress)
		if err != nil {
			log.Errorf("action: selectServer | result: fail | error: %v", err)
			h.currentIndex = (h.currentIndex + 1) % len(h.serverAddress)

			if h.currentIndex == startIndex {
				log.Errorf("action: selectServer | result: fail | error: no servers available after full iteration")
				return nil, ErrNoServersAvailable
			}
			continue
		}

		connected = true

		log.Infof("action: selectServer | result: success | server: %s", serverAddress)
	}

	return serverSocket, nil
}

func (h *Handler) Run() error {
	defer h.clientSocket.Close()
	defer h.serverSocket.Close()

	if err := h.handleConnection(); err != nil {
		if !h.isRunning {
			log.Infof("action: handleConnection | result: fail | error: %v", ErrSignalReceived)
		} else if err == client_server_communication.ErrConnectionClosed {
			log.Infof("action: handleConnection | result: fail | error: %v", err)
		} else {
			log.Errorf("action: handleConnection | result: fail | error: %v", err)
		}
		return err
	}

	return nil
}

func (h *Handler) handleConnection() error {
	for h.isRunning {

		err := h.redirectMessage()
		if err != nil {
			return err
		}
	}

	log.Infof("action: handleConnection | result: success | server socket closed")

	return nil
}

func (h *Handler) redirectMessage() error {

	messageSentToServer, err := h.redirectMessageToServer()
	if err != nil {
		log.Errorf("action: redirectMessage | result: fail | error: %v", err)
		return err
	}
	err = h.redirectMessageToClient(messageSentToServer)
	if err != nil {
		log.Errorf("action: redirectMessage | result: fail | error: %v", err)
		return err
	}

	return nil
}

func (h *Handler) redirectMessageToServer() (*protocol.Message, error) {
	message, err := h.clientSocket.Read()
	if err != nil {
		log.Errorf("action: handleMessage | result: fail | error: %v", err)
		return nil, err
	}

	err = h.sendMessageToServer(message)
	if err != nil {
		log.Errorf("action: handleMessage | result: fail | error: %v", err)
		return nil, err
	}

	return message, nil
}

func (h *Handler) redirectMessageToClient(messageSentToServer *protocol.Message) error {

	messageReceivedFromServer, err := h.readMessageFromServer(messageSentToServer)
	if err != nil {
		log.Errorf("action: redirectMessageToClient | result: fail | error: %v", err)
		return err
	}

	err = h.clientSocket.Write(messageReceivedFromServer)
	if err != nil {
		log.Errorf("action: handleMessage | result: fail | error: %v", err)
		return err
	}

	return nil
}

func (h *Handler) sendMessageToServer(message *protocol.Message) error {
	for alreadySent := false; !alreadySent; {
		err := h.serverSocket.Write(message)
		if err != nil {
			h.serverSocket.Close()

			// Try to select another server
			h.serverSocket, err = h.selectServer()
			if err != nil {
				return err
			}

			log.Infof("action: handleMessage | result: retry | trying another server")
			continue // Retry writing the message to the new server
		}

		alreadySent = true
	}
	return nil
}

func (h *Handler) readMessageFromServer(messageSentToServer *protocol.Message) (*protocol.Message, error) {

	var messageReadFromServer *protocol.Message
	var err error

	for alreadyRead := false; !alreadyRead; {

		messageReadFromServer, err = h.serverSocket.Read()
		if err != nil {
			h.serverSocket.Close()

			// Try to select another server
			h.serverSocket, err = h.selectServer()
			if err != nil {
				return nil, err
			}

			// Send again the message to the new server
			err = h.sendMessageToServer(messageSentToServer)
			if err != nil {
				log.Errorf("action: sendMessageToServer | result: fail | error: %v", err)
				return nil, err
			}

			continue
		}

		alreadyRead = true
	}

	return messageReadFromServer, nil
}

func (h *Handler) Stop() {
	h.isRunning = false

	if h.clientSocket != nil {
		h.clientSocket.Close()
		log.Infof("Client socket closed")
	}

	if h.serverSocket != nil {
		h.serverSocket.Close()
		log.Infof("Server socket closed")
	}

}
