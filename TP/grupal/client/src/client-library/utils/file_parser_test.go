package utils

import (
	"bufio"
	"io"
	"os"
	"testing"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/stretchr/testify/assert"
)

func TestParser_ReadBatch(t *testing.T) {
	t.Run("TestReadBatchWithMultipleLines", func(t *testing.T) {
		content := "metadata\nline1\nline2\nline3\n"
		file := createTempFile(t, content)
		defer os.Remove(file.Name())

		parser, err := NewParser(1000, file.Name(), protocol.FileType_MOVIES)
		assert.NoError(t, err)
		defer parser.Close()

		batch, err := parser.ReadBatch("", 0)
		assert.Equal(t, io.EOF, err)
		assert.NotNil(t, batch)

		rows := batch.GetMessage().(*protocol.Message_ClientServerMessage).ClientServerMessage.GetMessage().(*protocol.ClientServerMessage_Batch).Batch.Data
		assert.Equal(t, 3, len(rows))
		assert.Equal(t, "line1\n", rows[0].Data)
		assert.Equal(t, "line2\n", rows[1].Data)
		assert.Equal(t, "line3\n", rows[2].Data)
	})

	t.Run("TestReadBatchWithEOF", func(t *testing.T) {
		content := "metadata\nline1\nline2\n"
		file := createTempFile(t, content)
		defer os.Remove(file.Name())

		parser, err := NewParser(1000, file.Name(), protocol.FileType_MOVIES)
		assert.NoError(t, err)
		defer parser.Close()

		batch, err := parser.ReadBatch("", 0)
		assert.Equal(t, io.EOF, err)
		assert.NotNil(t, batch)

		rows := batch.GetMessage().(*protocol.Message_ClientServerMessage).ClientServerMessage.GetMessage().(*protocol.ClientServerMessage_Batch).Batch.Data
		assert.Equal(t, 2, len(rows))
		assert.Equal(t, "line1\n", rows[0].Data)
		assert.Equal(t, "line2\n", rows[1].Data)
	})
}

func createTempFile(t *testing.T, content string) *os.File {
	tempFile, err := os.CreateTemp("", "testfile")
	assert.NoError(t, err)

	writer := bufio.NewWriter(tempFile)
	_, err = writer.WriteString(content)
	assert.NoError(t, err)
	writer.Flush()

	tempFile.Close()
	tempFile, err = os.Open(tempFile.Name())
	assert.NoError(t, err)

	return tempFile
}
