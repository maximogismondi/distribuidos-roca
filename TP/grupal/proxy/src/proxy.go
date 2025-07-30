package proxy

import (
	"errors"
	"os"
	"os/exec"
	"strconv"
	"syscall"

	client_server_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server"
)

type Proxy struct {
	ID                      string
	connectionHandlerSocket *client_server_communication.Socket
	isRunning               bool
	connections             []*exec.Cmd
	serverAddresses         []string
	currentIndex            int
}

func NewConnectionHandler(id string, proxyAddress string, serverAddresses []string) (*Proxy, error) {
	proxySocket, err := client_server_communication.CreateServerSocket(proxyAddress)
	if err != nil {
		return nil, err
	}

	return &Proxy{
		ID:                      id,
		connectionHandlerSocket: proxySocket,
		isRunning:               true,
		connections:             make([]*exec.Cmd, 0),
		serverAddresses:         serverAddresses,
		currentIndex:            -1,
	}, nil
}

func (p *Proxy) AcceptConnections() error {
	for p.isRunning {
		p.cleanupFinishedClientHandlers()

		clientSocket, err := p.connectionHandlerSocket.Accept()
		if err != nil {
			if !p.isRunning {
				continue
			} else {
				log.Errorf("action: acceptConnections | result: fail | error: %v", err)
				return err
			}
		}

		defer clientSocket.Close()

		log.Infof("Client connected")

		err = p.HandleConnection(clientSocket)
		if err != nil {
			log.Errorf("action: handleConnection | result: fail | error: %v", err)
		}
	}

	return nil
}

func (p *Proxy) selectServer() error {
	if len(p.serverAddresses) == 0 {
		log.Panic("action: selectServer | result: fail | error: no servers available")
		return errors.New("no servers available")
	}

	p.currentIndex = (p.currentIndex + 1) % len(p.serverAddresses)

	return nil

}

func (p *Proxy) HandleConnection(clientSocket *client_server_communication.Socket) error {
	err := p.selectServer()
	if err != nil {
		log.Errorf("action: selectServer | result: fail | error: %v", err)
	}

	index := strconv.Itoa(p.currentIndex)

	cmd := exec.Command(os.Args[0], "child", index) // Fork the current binary
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

	p.connections = append(p.connections, cmd)

	return nil
}

func (p *Proxy) Run() error {
	return p.AcceptConnections()
}

func (p *Proxy) cleanupFinishedClientHandlers() {
	activeClients := make([]*exec.Cmd, 0)

	for _, client := range p.connections {
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

	p.connections = activeClients
}

func (p *Proxy) Stop() {
	p.isRunning = false

	if p.connectionHandlerSocket != nil {
		p.connectionHandlerSocket.Close()
		p.connectionHandlerSocket = nil
		log.Info("Server socket closed")
	}

	// signal
	for _, client := range p.connections {
		log.Infof("Sending SIGTERM to child process with PID %d", client.Process.Pid)
		err := client.Process.Signal(syscall.SIGTERM)

		if err != nil {
			log.Errorf("Failed to send SIGTERM to child process with PID %d: %v", client.Process.Pid, err)
			continue
		}
	}

	// wait
	for _, client := range p.connections {
		status, err := client.Process.Wait()

		if err != nil {
			log.Errorf("Failed to wait for child process with PID %d: %v", client.Process.Pid, err)
			continue
		}

		log.Infof("Child process with PID %d terminated, exit code: %d", status.Pid(), status.ExitCode())
	}
}
