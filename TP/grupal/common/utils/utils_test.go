package utils

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestGetWorkerIdFromHash(t *testing.T) {
	tests := []struct {
		workersCount int
		itemIdStr    string
		expectedHash string
	}{
		{workersCount: 5, itemIdStr: "10", expectedHash: "2"},
		{workersCount: 3, itemIdStr: "7", expectedHash: "0"},
		{workersCount: 4, itemIdStr: "USA", expectedHash: "0"},
		{workersCount: 10, itemIdStr: "ARG", expectedHash: "9"},
	}

	for _, test := range tests {
		hash := GetWorkerIdFromHash(test.workersCount, test.itemIdStr)
		assert.Equal(t, test.expectedHash, hash)
	}
}

func TestViperGetSliceMapStringString(t *testing.T) {
	data := map[string]any{
		"exchange1": map[string]any{
			"name": "exchange1",
			"type": "fanout",
		},
		"exchange2": map[string]any{
			"name": "exchange2",
			"type": "direct",
		},
	}

	result, err := ViperGetSliceMapStringString(data)
	assert.NoError(t, err)
	assert.Len(t, result, 2)
}

func TestViperGetSliceMapStringString_InvalidData(t *testing.T) {
	data := map[string]any{
		"exchange1": map[string]any{
			"name": "exchange1",
			"type": 123, // Invalid type
		},
	}

	_, err := ViperGetSliceMapStringString(data)
	assert.Error(t, err)
}

func TestGetRabbitConfig(t *testing.T) {
	v := viper.New()
	v.Set("rabbitmq.test.exchanges", map[string]any{
		"exchange1": map[string]any{
			"name": "exchange1",
			"type": "fanout",
		},
	})
	v.Set("rabbitmq.test.queues", map[string]any{
		"queue1": map[string]any{
			"name": "queue1",
		},
	})
	v.Set("rabbitmq.test.binds", map[string]any{
		"bind1": map[string]any{
			"exchange": "exchange1",
			"queue":    "queue1",
		},
	})

	exchanges, queues, binds, err := GetRabbitConfig("test", v)
	assert.NoError(t, err)

	expectedExchanges := []map[string]string{
		{"name": "exchange1", "type": "fanout"},
	}
	expectedQueues := []map[string]string{
		{"name": "queue1"},
	}
	expectedBinds := []map[string]string{
		{"exchange": "exchange1", "queue": "queue1"},
	}

	assert.Equal(t, expectedExchanges, exchanges)
	assert.Equal(t, expectedQueues, queues)
	assert.Equal(t, expectedBinds, binds)
}

func TestGetRabbitConfig_InvalidData(t *testing.T) {
	v := viper.New()
	v.Set("rabbitmq.test.exchanges", map[string]any{
		"exchange1": map[string]any{
			"name": "exchange1",
			"type": 123, // Invalid type
		},
	})

	_, _, _, err := GetRabbitConfig("test", v)
	assert.Error(t, err)
}
