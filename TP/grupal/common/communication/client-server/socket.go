package client_server_communication

import (
	"bufio"
	"fmt"
	"net"
	"os"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/op/go-logging"
	"google.golang.org/protobuf/proto"
)

var log = logging.MustGetLogger("log")
var ErrConnectionClosed = fmt.Errorf("client-server connection closed")

type Socket struct {
	conn     net.Conn
	reader   *bufio.Reader
	listener net.Listener
}

func Connect(address string) (*Socket, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	clientSocket := &Socket{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		listener: nil,
	}

	return clientSocket, nil
}

func CreateServerSocket(address string) (*Socket, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	serverSocket := &Socket{
		conn:     nil,
		reader:   nil,
		listener: listener,
	}

	return serverSocket, nil
}

func (s *Socket) Accept() (*Socket, error) {
	conn, err := s.listener.Accept()
	if err != nil {

		return nil, err
	}

	clientSocket := &Socket{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}

	return clientSocket, nil
}

// GetFileDescriptor returns the file descriptor of the socket connection
// It returns as *os.File. This is useful for passing the file descriptor to
// child processes or for using with system calls that require a file descriptor.
// Note: This function is only valid for TCP connections. For other types of connections,
// it may not work as expected.
// It returns an error if the connection type is not *net.TCPConn.
// The caller is responsible for closing the file descriptor when done
// in the child process.
func (s *Socket) GetFileDescriptor() (*os.File, error) {
	switch c := s.conn.(type) {
	case *net.TCPConn:
		file, err := c.File()

		if err != nil {
			return nil, err
		}

		return file, nil

	default:
		return nil, fmt.Errorf("connection type should be *net.TCPConn, got %T", c)
	}
}

// NewClientSocketFromFile creates a new Socket from a file descriptor
// It takes a file descriptor as input and returns a new Socket.
func NewClientSocketFromFile(fd uintptr) (*Socket, error) {
	file := os.NewFile(fd, "clientSocket")
	if file == nil {
		return nil, fmt.Errorf(
			"failed to create file from file descriptor, fd %d is not valid", fd,
		)
	}

	defer file.Close()

	conn, err := net.FileConn(file)
	if err != nil {
		return nil, err
	}

	clientSocket := &Socket{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		listener: nil,
	}

	return clientSocket, nil
}

func (s *Socket) Read() (*protocol.Message, error) {
	expectedLength := 3
	lengthBytes := make([]byte, expectedLength)
	for expectedLength > 0 {
		n, err := s.reader.Read(lengthBytes[len(lengthBytes)-expectedLength:])
		expectedLength -= n

		if err != nil {
			if err.Error() == "EOF" || err.Error() == "use of closed network connection" {
				return nil, ErrConnectionClosed
			} else {
				return nil, err
			}
		}
	}

	length := int(lengthBytes[0])<<16 | int(lengthBytes[1])<<8 | int(lengthBytes[2])
	log.Debugf("Message length: %d", length)

	message := make([]byte, length)

	messageReceivedLength := 0

	for messageReceivedLength < length {
		n, err := s.reader.Read(message[messageReceivedLength:])
		if err != nil {
			if err.Error() == "EOF" || err.Error() == "use of closed network connection" {
				return nil, ErrConnectionClosed
			} else {
				return nil, err
			}
		}
		messageReceivedLength += n
	}

	// Deserializa el mensaje usando proto.Unmarshal
	responseMessage := &protocol.Message{}
	err := proto.Unmarshal(message, responseMessage)
	if err != nil {
		return nil, err
	}

	//log.Debugf("Successfully deserialized message: %v", responseMessage)
	return responseMessage, nil
}

func (s *Socket) Write(message *protocol.Message) error {

	message_bytes, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	// log.Debugf("Sending message: %v", message_bytes)

	length := len(message_bytes)

	sendMessage := make([]byte, 3+length)

	// Write the length of the message in the first 3 bytes
	sendMessage[0] = byte(length >> 16)
	sendMessage[1] = byte(length >> 8)
	sendMessage[2] = byte(length & 0xFF)

	copy(sendMessage[3:], message_bytes)

	bytes_written := 0
	for bytes_written < len(sendMessage) {
		n, err := s.conn.Write(sendMessage[bytes_written:])
		if err != nil {
			if err.Error() == "EOF" || err.Error() == "use of closed network connection" {
				return ErrConnectionClosed
			} else {
				return err
			}
		}
		bytes_written += n
	}
	return nil
}

func (s *Socket) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil

}
