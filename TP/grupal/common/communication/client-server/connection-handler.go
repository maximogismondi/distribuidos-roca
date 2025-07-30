package client_server_communication

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// var log = logging.MustGetLogger("log")
var ErrSignalReceived = errors.New("signal received")

type ConnectionHandler struct {
	ID                      string
	connectionHandlerSocket *Socket
	isRunning               bool
	connections             []*exec.Cmd
}

func NewConnectionHandler(id string, address string) (*ConnectionHandler, error) {
	serverSocket, err := CreateServerSocket(address)
	if err != nil {
		return nil, err
	}

	return &ConnectionHandler{
		ID:                      id,
		connectionHandlerSocket: serverSocket,
		isRunning:               true,
		connections:             make([]*exec.Cmd, 0),
	}, nil
}
func (ch *ConnectionHandler) IsRunning() bool {
	return ch.isRunning
}

func (ch *ConnectionHandler) AcceptConnections() error {
	for ch.isRunning {
		ch.cleanupFinishedClientHandlers()

		clientSocket, err := ch.connectionHandlerSocket.Accept()
		if err != nil {
			if !ch.isRunning {
				continue
			} else {
				log.Errorf("action: acceptConnections | result: fail | error: %v", err)
				return err
			}
		}

		defer clientSocket.Close()

		log.Infof("Client connected")

		err = ch.HandleConnection(clientSocket)
		if err != nil {
			log.Errorf("action: handleConnection | result: fail | error: %v", err)
		}
	}

	return nil
}

func (ch *ConnectionHandler) HandleConnection(clientSocket *Socket) error {
	cmd := exec.Command(os.Args[0], "child") // Fork the current binary
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	socketFD, err := clientSocket.GetFileDescriptor()

	if err != nil {
		log.Errorf("action: getFileDescriptor | result: fail | error: %v", err)
		return err
	}

	cmd.ExtraFiles = []*os.File{socketFD}

	err = cmd.Start()
	if err != nil {
		log.Errorf("action: forkChildProcess | result: fail | error: %v", err)
		return err
	}

	log.Infof("Child process started with PID: %d", cmd.Process.Pid)

	ch.connections = append(ch.connections, cmd)

	return nil
}

func (ch *ConnectionHandler) Run() error {
	return ch.AcceptConnections()
}

func (ch *ConnectionHandler) cleanupFinishedClientHandlers() {
	activeClients := make([]*exec.Cmd, 0)

	for _, client := range ch.connections {
		// Check if the process has finished
		var status syscall.WaitStatus
		wpid, err := syscall.Wait4(client.Process.Pid, &status, syscall.WNOHANG, nil)

		if wpid == client.Process.Pid {
			log.Infof("Child process with PID %d terminated, exit code: %d", wpid, status.ExitStatus())
		} else if wpid == 0 {
			// Process is still running
			activeClients = append(activeClients, client)
		} else {
			log.Errorf("Unexpected PID %d returned from Wait4, expected %d, %v", wpid, client.Process.Pid, err)
			activeClients = append(activeClients, client)
		}
	}

	ch.connections = activeClients
}

func (ch *ConnectionHandler) Stop() {
	ch.isRunning = false

	if ch.connectionHandlerSocket != nil {
		ch.connectionHandlerSocket.Close()
		ch.connectionHandlerSocket = nil
		log.Info("Server socket closed")
	}

	// signal
	for _, client := range ch.connections {
		log.Infof("Sending SIGTERM to child process with PID %d", client.Process.Pid)
		err := client.Process.Signal(syscall.SIGTERM)

		if err != nil {
			log.Errorf("Failed to send SIGTERM to child process with PID %d: %v", client.Process.Pid, err)
			continue
		}
	}

	// wait
	for _, client := range ch.connections {
		status, err := client.Process.Wait()

		if err != nil {
			log.Errorf("Failed to wait for child process with PID %d: %v", client.Process.Pid, err)
			continue
		}

		log.Infof("Child process with PID %d terminated, exit code: %d", status.Pid(), status.ExitCode())
	}
}
