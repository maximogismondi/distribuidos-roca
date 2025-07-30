package library

import (
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/MaxiOtero6/TP-Distribuidos/client/src/client-library/model"
	"github.com/MaxiOtero6/TP-Distribuidos/client/src/client-library/utils"
	client_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/op/go-logging"
)

const SLEEP_TIME time.Duration = 5 * time.Second
const MAX_MULTIPLIER float64 = 20.0
const DELAY_MULTIPLIER int = 2
const MAX_RETRIES int = 50

var log = logging.MustGetLogger("log")

var ErrSignalReceived = errors.New("signal received")

type ClientConfig struct {
	ServerAddress   string
	BatchMaxSize    int
	MoviesFilePath  string
	RatingsFilePath string
	CreditsFilePath string
	ClientId        string
}

type Library struct {
	socket        *client_communication.Socket
	retryNumber   int
	responseCount int
	isRunning     bool
	config        ClientConfig
}

func NewLibrary(config ClientConfig) (*Library, error) {

	socket, err := client_communication.Connect(config.ServerAddress)
	if err != nil {
		return nil, err
	}

	return &Library{
		socket:        socket,
		config:        config,
		retryNumber:   0,
		isRunning:     true,
		responseCount: 0,
	}, nil
}

func (l *Library) ProcessData() (results *model.Results) {
	err := l.sendSync()
	if err != nil {
		if err == client_communication.ErrConnectionClosed {
			log.Errorf("action: sendSync | result: fail | error: %v", err)
			return
		}
		log.Errorf("action: sendSync | result: fail | error: %v", err)
		return
	}

	err = l.sendAllFiles()
	if err != nil {
		if err == client_communication.ErrConnectionClosed {
			log.Errorf("action: sendAllFiles | result: fail | error: %v", err)
			return
		}
		log.Errorf("action: sendAllFiles | result: fail | error: %v", err)
		return
	}

	results, err = l.fetchServerResults()

	if err != nil {
		if err == ErrSignalReceived {
			log.Infof("action: fetchServerResults | result: success | error: %v", err)
			return
		} else if err == client_communication.ErrConnectionClosed {
			log.Errorf("action: fetchServerResults | result: fail | error: %v", err)
			return
		}

		log.Errorf("action: fetchServerResults | result: fail | error: %v", err)
		return
	}

	if results == nil && l.isRunning {
		log.Errorf("action: fetchServerResults | result: fail | error: results is nil")
	}

	return results
}

func (l *Library) sendAllFiles() error {

	err := l.sendMoviesFile()
	if err != nil {
		return err
	}

	err = l.sendCreditsFile()
	if err != nil {
		return err
	}
	err = l.sendRatingsFile()
	if err != nil {
		return err
	}

	return nil
}

func (l *Library) sendFileEOF(fileType protocol.FileType, batchCount int, filename string) error {
	fileEof := &protocol.Message{
		Message: &protocol.Message_ClientServerMessage{
			ClientServerMessage: &protocol.ClientServerMessage{
				Message: &protocol.ClientServerMessage_FileEof{
					FileEof: &protocol.FileEOF{
						ClientId:   l.config.ClientId,
						Type:       fileType,
						BatchCount: uint32(batchCount),
					},
				},
			},
		},
	}

	if err := l.socket.Write(fileEof); err != nil {
		return err
	}

	log.Debugf("action: sendFile | result: file_eof_sent | clientId: %v | file: %s | batchCount: %d", l.config.ClientId, filename, batchCount)

	if err := l.waitACK(); err != nil {
		log.Errorf("action: waitFileEofACK | result: fail | clientId: %v | file: %s", l.config.ClientId, filename)
		return err
	}

	log.Infof("action: sendFile | result: file_eof_ack_received | clientId: %v | file: %s", l.config.ClientId, filename)
	return nil
}

// Read line by line the file with the name pass by parameter and
// Send each line to the server and wait for confirmation
// until an OEF or the client is shutdown
func (l *Library) sendFile(filename string, fileType protocol.FileType) error {
	log.Infof("action: sendFile | result: start | file: %s", filename)

	parser, err := utils.NewParser(l.config.BatchMaxSize, filename, fileType)
	if err != nil {
		return err
	}

	defer parser.Close()

	batchCount := 0

	for l.isRunning {
		batch, fileErr := parser.ReadBatch(l.config.ClientId, batchCount)

		if fileErr == io.EOF {
			log.Infof("action: sendFile | result: end | file: %s", filename)
			return l.sendFileEOF(fileType, batchCount, filename)
		}

		if fileErr != nil {
			return err
		}

		batchCount++

		if err := l.socket.Write(batch); err != nil {
			return err
		}

		log.Debugf("action: batch_send | result: success | clientId: %v |  file: %s", l.config.ClientId, filename)

		if err := l.waitACK(); err != nil {
			log.Criticalf("action: batch_send | result: fail | clientId: %v | file: %s", l.config.ClientId, filename)
			return err
		}
	}

	return nil
}

func (l *Library) sendMoviesFile() error {
	err := l.sendFile(l.config.MoviesFilePath, protocol.FileType_MOVIES)
	if err != nil {
		return err
	}
	return nil
}
func (l *Library) sendRatingsFile() error {
	err := l.sendFile(l.config.RatingsFilePath, protocol.FileType_RATINGS)
	if err != nil {
		return err
	}
	return nil
}
func (l *Library) sendCreditsFile() error {
	err := l.sendFile(l.config.CreditsFilePath, protocol.FileType_CREDITS)
	if err != nil {
		return err
	}
	return nil
}

func (l *Library) sendSync() error {
	if !l.isRunning {
		return nil
	}

	syncMessage := &protocol.Message{
		Message: &protocol.Message_ClientServerMessage{
			ClientServerMessage: &protocol.ClientServerMessage{
				Message: &protocol.ClientServerMessage_Sync{
					Sync: &protocol.Sync{
						ClientId: l.config.ClientId,
					},
				},
			},
		},
	}

	if err := l.socket.Write(syncMessage); err != nil {
		return err
	}

	if err := l.waitACK(); err != nil {
		return err
	}

	return nil
}
func (l *Library) sendFinishMessage() error {
	if l.socket == nil {
		log.Debugf("action: sendFinishMessage | result: fail | error: socket is nil")
		return nil
	}

	if !l.isRunning {
		return nil
	}

	finishMessage := &protocol.Message{
		Message: &protocol.Message_ClientServerMessage{
			ClientServerMessage: &protocol.ClientServerMessage{
				Message: &protocol.ClientServerMessage_Finish{
					Finish: &protocol.Finish{
						ClientId: l.config.ClientId,
					},
				},
			},
		},
	}

	if err := l.socket.Write(finishMessage); err != nil {
		return err
	}
	log.Infof("action: SendFinishMessage | result: success | client_id: %v", l.config.ClientId)
	return nil
}

func (l *Library) sendDisconnectMessage() error {
	if !l.isRunning {
		return nil
	}

	disconnectMessage := &protocol.Message{
		Message: &protocol.Message_ClientServerMessage{
			ClientServerMessage: &protocol.ClientServerMessage{
				Message: &protocol.ClientServerMessage_Disconnect{
					Disconnect: &protocol.Disconnect{
						ClientId: l.config.ClientId,
					},
				},
			},
		},
	}

	if err := l.socket.Write(disconnectMessage); err != nil {
		return err
	}
	log.Infof("action: SendDisconnectMessage | result: success | client_id: %v", l.config.ClientId)
	return nil
}

func (l *Library) fetchServerResults() (*model.Results, error) {
	resultParser := utils.NewResultParser()

	for l.isRunning && !resultParser.IsDone() {

		if l.retryNumber > MAX_RETRIES {
			return resultParser.GetResults(), fmt.Errorf("timeout")
		}

		if l.socket == nil {
			if err := l.connectToServer(); err != nil {
				return resultParser.GetResults(), err
			}
		}

		start := time.Now()
		err := l.sendResultMessage()
		if err != nil {
			return resultParser.GetResults(), err
		}

		ok, response, err := l.waitForResultServerResponse()

		if err != nil {
			return resultParser.GetResults(), err
		}

		jitter := time.Since(start)

		log.Debugf("action: waitForResultServerResponse | result: success | response: %v", response)

		if !ok {
			l.sendDisconnectMessage()
			if err := l.waitACK(); err != nil {
				log.Errorf("action: waitDisconnectACK | result: fail | error: %v", err)
			}
			l.disconnectFromServer()

			// Exponential backoff + jitter
			sleepTime := SLEEP_TIME*(time.Duration(math.Min(math.Pow(2.0, float64(l.retryNumber)), MAX_MULTIPLIER))) + jitter
			l.retryNumber++
			log.Warningf("action: waitForResultServerResponse | result: fail | retryNumber: %v | sleepTime: %v", l.retryNumber, sleepTime)
			time.Sleep(sleepTime)
			continue
		}

		resultParser.Save(response)
	}

	return resultParser.GetResults(), nil
}

func (l *Library) waitForResultServerResponse() (bool, *protocol.ResultsResponse, error) {
	response, err := l.waitForServerResponse()
	if err != nil {
		return false, nil, err
	}

	switch resp := response.GetMessage().(type) {
	case *protocol.ServerClientMessage_Results:
		switch resp.Results.Status {
		case protocol.MessageStatus_SUCCESS:
			return true, response.GetResults(), nil
		case protocol.MessageStatus_PENDING:
			return false, nil, nil
		}
	}
	return false, nil, fmt.Errorf("unexpected response type received")

}

func (l *Library) connectToServer() error {
	var err error
	l.socket, err = client_communication.Connect(l.config.ServerAddress)

	if err != nil {
		log.Infof("action: ConnectToServer | result: false | address: %v", l.config.ServerAddress)
		return err
	}

	log.Infof("action: ConnectToServer | result: success | address: %v", l.config.ServerAddress)

	err = l.sendSync()
	if err != nil {
		return err
	}

	return nil
}
func (l *Library) disconnectFromServer() {
	if l.socket != nil {
		l.socket.Close()
		log.Infof("action: disconnectFromServer | result: success | address: %v", l.config.ServerAddress)
	}
	l.socket = nil
}

func (l *Library) waitForServerResponse() (*protocol.ServerClientMessage, error) {
	log.Debugf("action: waitForServerResponse | result: start")
	response, err := l.socket.Read()
	if err != nil {
		if !l.isRunning {
			return nil, ErrSignalReceived
		}
		return nil, err
	}

	serverClientMessage, ok := response.GetMessage().(*protocol.Message_ServerClientMessage)
	if !ok {
		return nil, fmt.Errorf("unexpected message type: expected ServerClientMessage")
	}

	log.Debugf("action: waitForServerResponse | result: success")
	return serverClientMessage.ServerClientMessage, nil
}

func (l *Library) waitACK() error {
	if l.socket == nil {
		log.Debugf("action: waitACK | result: fail | error: socket is nil")
		return nil
	}

	response, err := l.waitForServerResponse()
	if err != nil {
		return err
	}

	switch resp := response.GetMessage().(type) {
	case *protocol.ServerClientMessage_BatchAck:
		if resp.BatchAck.Status == protocol.MessageStatus_SUCCESS {
			log.Debugf("action: receiveBatchAckResponse | result: success | response: %v", resp.BatchAck.Status)

		} else {
			log.Errorf("action: receiveBatchAckResponse | result: fail | error: %v", resp.BatchAck.Status)
			return fmt.Errorf("server response was not succes")
		}
	case *protocol.ServerClientMessage_FileEofAck:
		if resp.FileEofAck.Status == protocol.MessageStatus_SUCCESS {
			log.Debugf("action: receiveFileEofAckResponse | result: success | response: %v", resp.FileEofAck.Status)
		} else {
			log.Errorf("action: receiveFileEofAckResponse | result: fail | error: %v", resp.FileEofAck.Status)
			return fmt.Errorf("server response was not success")
		}
	case *protocol.ServerClientMessage_SyncAck:
		l.config.ClientId = resp.SyncAck.ClientId
		log.Debugf("action: receiveSyncAckResponse | result: success | clientId: %v", l.config.ClientId)

	case *protocol.ServerClientMessage_FinishAck:
		log.Debugf("action: receiveFinishAckResponse | result: success | clientId: %v", l.config.ClientId)

	case *protocol.ServerClientMessage_DisconnectAck:
		log.Debugf("action: receiveDisconnectAckResponse | result: success | clientId: %v", l.config.ClientId)

	default:
		log.Errorf("action: receiveResponse | result: fail | error: Unexpected response type received")
		return fmt.Errorf("unexpected response type received")
	}

	return nil
}

func (l *Library) sendResultMessage() error {
	consultMessage := &protocol.Message{
		Message: &protocol.Message_ClientServerMessage{
			ClientServerMessage: &protocol.ClientServerMessage{
				Message: &protocol.ClientServerMessage_Result{
					Result: &protocol.Result{
						ClientId: l.config.ClientId,
					},
				},
			},
		},
	}

	if err := l.socket.Write(consultMessage); err != nil {
		return err
	}

	log.Debugf("action: sendResultMessage | result: success | clientId: %v", l.config.ClientId)

	return nil
}

func (l *Library) Stop() {
	l.sendFinishMessage()
	err := l.waitACK()
	if err != nil {
		log.Errorf("action: waitFinishACK | result: fail | error: %v", err)
	}

	l.isRunning = false
	l.disconnectFromServer()
}

func (l *Library) DumpResultsToJson(filePath string, results *model.Results) {
	utils.DumpResultsToJson(filePath, results)
}
