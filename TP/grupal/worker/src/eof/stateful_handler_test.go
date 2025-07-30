package eof_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof"
)

func TestMergeRingEOFFragments(t *testing.T) {
	ringEOF := &protocol.RingEOF{
		StageFragmentes: []*protocol.StageFragment{
			{
				CreatorId:    "c27939b0-4bdd-4a18-a08e-9978cf93a96f",
				Start:        &protocol.FragmentIdentifier{},
				End:          &protocol.FragmentIdentifier{TaskNumber: 100},
				LastFragment: true,
			},
			{
				CreatorId:    "c27939b0-4bdd-4a18-a08e-9978cf93a96f",
				Start:        &protocol.FragmentIdentifier{},
				End:          &protocol.FragmentIdentifier{TaskNumber: 100},
				LastFragment: true,
			},
			{
				CreatorId:    "c27939b0-4bdd-4a18-a08e-9978cf93a96f",
				Start:        &protocol.FragmentIdentifier{},
				End:          &protocol.FragmentIdentifier{TaskNumber: 100},
				LastFragment: true,
			},
			{
				CreatorId:    "c27939b0-4bdd-4a18-a08e-9978cf93a96f",
				Start:        &protocol.FragmentIdentifier{},
				End:          &protocol.FragmentIdentifier{TaskNumber: 1},
				LastFragment: true,
			},
			{
				CreatorId:    "c27939b0-4bdd-4a18-a08e-9978cf93a96f",
				Start:        &protocol.FragmentIdentifier{},
				End:          &protocol.FragmentIdentifier{TaskNumber: 98, TaskFragmentNumber: 1},
				LastFragment: true,
			},
			{
				CreatorId:    "c27939b0-4bdd-4a18-a08e-9978cf93a96f",
				Start:        &protocol.FragmentIdentifier{TaskNumber: 2},
				End:          &protocol.FragmentIdentifier{TaskNumber: 100},
				LastFragment: true,
			},
		},
	}

	i := model.NewInfraConfig(
		"0",
		&model.WorkerClusterConfig{
			FilterCount: 1,
			MapCount:    1,
			ReduceCount: 3,
		},
		&model.RabbitConfig{
			FilterExchange: "filterExchange",
			JoinExchange:   "joinExchange",
			ResultExchange: "resultExchange",
			MapExchange:    "mapExchange",
			ReduceExchange: "reduceExchange",
			BroadcastID:    "",
		},
		"",
		0,
	)

	nextStageFunc := func(stage string, clientId string, _ *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
		return []common.NextStageData{
			{
				Stage:       stage,
				Exchange:    "",
				WorkerCount: 1,
				RoutingKey:  "",
			},
		}, nil
	}

	hashFunc := utils.GetWorkerIdFromHash

	eofHandler := eof.NewStatefulEofHandler(model.ReducerAction, i, nextStageFunc, hashFunc)
	eofHandler.MergeStageFragmentsPublic(ringEOF, []model.TaskFragmentIdentifier{})

	assert.Equal(t, 1, len(ringEOF.StageFragmentes), "Expected only one merged fragment")

	// Aquí se pueden agregar los asserts para verificar el resultado después de aplicar la lógica de merge
}

func TestFullTest(t *testing.T) {

	i := model.NewInfraConfig(
		"0",
		&model.WorkerClusterConfig{
			FilterCount: 1,
			MapCount:    1,
			ReduceCount: 3,
		},
		&model.RabbitConfig{
			FilterExchange: "filterExchange",
			JoinExchange:   "joinExchange",
			ResultExchange: "resultExchange",
			MapExchange:    "mapExchange",
			ReduceExchange: "reduceExchange",
			BroadcastID:    "",
		},
		"",
		0,
	)

	nextStageFunc := func(stage string, clientId string, _ *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
		return []common.NextStageData{
			{
				Stage:       stage,
				Exchange:    "",
				WorkerCount: 1,
				RoutingKey:  "",
			},
		}, nil
	}

	hashFunc := utils.GetWorkerIdFromHash

	eofHandler := eof.NewStatefulEofHandler(model.ReducerAction, i, nextStageFunc, hashFunc)

	// random de 5 a 10 workers
	CANT_WORKERS := rand.Intn(5) + 5
	CANT_TASKS := rand.Intn(300) + 100

	ringEOF := &protocol.RingEOF{
		Stage: "test_stage",
	}

	tasks_identifiers := make([]model.TaskFragmentIdentifier, 0)

	for i := range CANT_TASKS {
		fragments := rand.Intn(CANT_WORKERS) + 1

		for j := range fragments {
			tasks_identifiers = append(tasks_identifiers, model.TaskFragmentIdentifier{
				CreatorId:          "pepe",
				TaskNumber:         uint32(i),
				TaskFragmentNumber: uint32(j),
				LastFragment:       j == fragments-1,
			})
		}
	}

	SHUFFLE_TIMES := 1000

	// shuffle tasks_identifiers
	for range SHUFFLE_TIMES {
		for i := range tasks_identifiers {
			j := rand.Intn(i + 1)
			tasks_identifiers[i], tasks_identifiers[j] = tasks_identifiers[j], tasks_identifiers[i]
		}
	}

	for i := range tasks_identifiers {
		tempTaskIdentifiers := tasks_identifiers[0 : i+1]
		eofHandler.MergeStageFragmentsPublic(ringEOF, tempTaskIdentifiers)
	}

	assert.Len(t, ringEOF.StageFragmentes, 1, "Expected only one merged fragment after processing all task identifiers")
	fmt.Println("Final merged fragments:", ringEOF.StageFragmentes)

	// This test is a placeholder to ensure the test suite runs without errors.
	// It can be expanded with more specific tests as needed.
	// assert.True(t, true, "This is a placeholder test to ensure the test suite runs without errors.")
}

func TestMergeStageFragmentsWithRealLogData(t *testing.T) {
	// Set up test infrastructure
	i := model.NewInfraConfig(
		"0",
		&model.WorkerClusterConfig{
			FilterCount: 1,
			MapCount:    1,
			ReduceCount: 3,
		},
		&model.RabbitConfig{
			FilterExchange: "filterExchange",
			JoinExchange:   "joinExchange",
			ResultExchange: "resultExchange",
			MapExchange:    "mapExchange",
			ReduceExchange: "reduceExchange",
			BroadcastID:    "",
		},
		"",
		0,
	)

	nextStageFunc := func(stage string, clientId string, _ *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
		return []common.NextStageData{
			{
				Stage:       stage,
				Exchange:    "",
				WorkerCount: 1,
				RoutingKey:  "",
			},
		}, nil
	}

	hashFunc := utils.GetWorkerIdFromHash
	eofHandler := eof.NewStatefulEofHandler(model.ReducerAction, i, nextStageFunc, hashFunc)

	// Create task identifiers from the log data

	tasks_identifiers := []model.TaskFragmentIdentifier{
		{CreatorId: "0", TaskNumber: 300, LastFragment: true},
		{CreatorId: "1", TaskNumber: 368, LastFragment: true},
		{CreatorId: "1", TaskNumber: 371, LastFragment: true},
		{CreatorId: "0", TaskNumber: 406, LastFragment: true},
		{CreatorId: "1", TaskNumber: 428, LastFragment: true},
		{CreatorId: "1", TaskNumber: 444, LastFragment: true},
		{CreatorId: "0", TaskNumber: 464, LastFragment: true},
		{CreatorId: "1", TaskNumber: 472, LastFragment: true},
		{CreatorId: "1", TaskNumber: 502, LastFragment: true},
		{CreatorId: "0", TaskNumber: 535, LastFragment: true},
		{CreatorId: "0", TaskNumber: 561, LastFragment: true},
		{CreatorId: "1", TaskNumber: 610, LastFragment: true},
		{CreatorId: "0", TaskNumber: 623, LastFragment: true},
		{CreatorId: "1", TaskNumber: 698, LastFragment: true},
		{CreatorId: "1", TaskNumber: 701, LastFragment: true},
		{CreatorId: "1", TaskNumber: 735, LastFragment: true},
		{CreatorId: "1", TaskNumber: 762, LastFragment: true},
		{CreatorId: "1", TaskNumber: 783, LastFragment: true},
		{CreatorId: "0", TaskNumber: 789, LastFragment: true},
		{CreatorId: "1", TaskNumber: 803, LastFragment: true},
		{CreatorId: "1", TaskNumber: 846, LastFragment: true},
		{CreatorId: "1", TaskNumber: 854, LastFragment: true},
		{CreatorId: "0", TaskNumber: 895, LastFragment: true},
		{CreatorId: "0", TaskNumber: 903, LastFragment: true},
		{CreatorId: "1", TaskNumber: 910, LastFragment: true},
		{CreatorId: "0", TaskNumber: 916, LastFragment: true},
		{CreatorId: "1", TaskNumber: 951, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1010, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1014, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1019, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1056, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1102, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1127, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1135, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1137, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1141, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1145, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1146, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1154, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1205, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1210, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1212, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1244, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1249, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1259, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1270, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1277, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1279, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1293, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1319, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1333, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1341, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1347, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1348, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1360, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1365, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1366, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1382, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1385, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1389, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1422, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1429, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1434, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1436, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1441, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1450, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1457, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1462, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1479, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1501, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1529, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1533, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1534, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1538, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1571, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1577, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1578, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1587, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1607, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1623, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1624, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1627, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1644, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1674, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1680, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1692, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1697, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1699, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1705, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1720, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1734, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1737, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1750, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1757, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1773, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1781},
		{CreatorId: "0", TaskNumber: 1781},
		{CreatorId: "0", TaskNumber: 1810, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1811, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1814, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1831, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1873, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1922, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1923, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1932, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1939, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1940, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1943, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1948, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1959, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1960, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1965, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1970, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1974, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2007, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2008, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2028, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2032, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2040, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2045, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2067},
		{CreatorId: "1", TaskNumber: 2067},
		{CreatorId: "1", TaskNumber: 2073, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2077, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2079, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2092, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2093, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2127, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2128, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2135, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2140, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2145},
		{CreatorId: "1", TaskNumber: 2145},
		{CreatorId: "1", TaskNumber: 2150, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2164, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2165, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2173, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2176, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2193, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2196, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2201, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2222, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2243, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2247, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2248, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2263, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2280, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2291, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2301, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2310, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2322, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2323},
		{CreatorId: "1", TaskNumber: 2323, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2327, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2355, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2358, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2379, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2392, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2408, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2410, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2429, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2431, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2432, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2441, LastFragment: true},
		{CreatorId: "1", TaskNumber: 300, LastFragment: true},
		{CreatorId: "0", TaskNumber: 368, LastFragment: true},
		{CreatorId: "0", TaskNumber: 371, LastFragment: true},
		{CreatorId: "1", TaskNumber: 406, LastFragment: true},
		{CreatorId: "0", TaskNumber: 428, LastFragment: true},
		{CreatorId: "0", TaskNumber: 444, LastFragment: true},
		{CreatorId: "1", TaskNumber: 464, LastFragment: true},
		{CreatorId: "0", TaskNumber: 472, LastFragment: true},
		{CreatorId: "0", TaskNumber: 502, LastFragment: true},
		{CreatorId: "1", TaskNumber: 535, LastFragment: true},
		{CreatorId: "1", TaskNumber: 561, LastFragment: true},
		{CreatorId: "0", TaskNumber: 610, LastFragment: true},
		{CreatorId: "1", TaskNumber: 623, LastFragment: true},
		{CreatorId: "0", TaskNumber: 698, LastFragment: true},
		{CreatorId: "0", TaskNumber: 701, LastFragment: true},
		{CreatorId: "0", TaskNumber: 735, LastFragment: true},
		{CreatorId: "0", TaskNumber: 762, LastFragment: true},
		{CreatorId: "0", TaskNumber: 783, LastFragment: true},
		{CreatorId: "1", TaskNumber: 789, LastFragment: true},
		{CreatorId: "0", TaskNumber: 803, LastFragment: true},
		{CreatorId: "0", TaskNumber: 846, LastFragment: true},
		{CreatorId: "0", TaskNumber: 854, LastFragment: true},
		{CreatorId: "1", TaskNumber: 895, LastFragment: true},
		{CreatorId: "1", TaskNumber: 903, LastFragment: true},
		{CreatorId: "0", TaskNumber: 910, LastFragment: true},
		{CreatorId: "1", TaskNumber: 916, LastFragment: true},
		{CreatorId: "0", TaskNumber: 951, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1010, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1014, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1019, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1048, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1056, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1102, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1127, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1135, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1137, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1141, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1145, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1146, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1154, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1165, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1205, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1210, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1212, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1244, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1249, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1259, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1277, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1279, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1293, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1319, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1333, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1341, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1347, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1348, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1360, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1365, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1366, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1382, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1385, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1389, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1422, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1429, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1434, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1436, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1441, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1450, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1457, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1462, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1479, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1501, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1529, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1533, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1534, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1571, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1577, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1578, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1587, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1607, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1623, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1624, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1627, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1644, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1674, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1680, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1692, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1697, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1699, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1705, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1720, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1734, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1737, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1750, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1757, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1777, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1781, TaskFragmentNumber: 1, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1781, TaskFragmentNumber: 1, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1810, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1811, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1814, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1831, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1873, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1920, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1922, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1923, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1932, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1939, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1940, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1943, LastFragment: true},
		{CreatorId: "1", TaskNumber: 1948, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1960, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1959, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1965, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1970, LastFragment: true},
		{CreatorId: "0", TaskNumber: 1974, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2007, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2008, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2028, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2040, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2045, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2067, TaskFragmentNumber: 1, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2067, TaskFragmentNumber: 1, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2073, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2079, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2077, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2092, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2093, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2127, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2128, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2135, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2140, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2145, TaskFragmentNumber: 1, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2145, TaskFragmentNumber: 1, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2150, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2164, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2165, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2171, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2173, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2196, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2201, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2222, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2243, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2247, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2248, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2263, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2271, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2280, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2291, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2301, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2310, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2322, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2323, TaskFragmentNumber: 1, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2327, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2355, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2356, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2379, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2392, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2408, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2410, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2429, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2431, LastFragment: true},
		{CreatorId: "0", TaskNumber: 2432, LastFragment: true},
		{CreatorId: "1", TaskNumber: 2441, LastFragment: true},
	}

	// Test scenario 1: Process all fragments at once
	ringEOF := &protocol.RingEOF{
		Stage:      "kappa_2",
		TasksCount: 320,
	}

	eofHandler.MergeStageFragmentsPublic(ringEOF, tasks_identifiers)

	stageFragments := ringEOF.GetStageFragmentes()

	totalTasks := 0
	for _, frag := range stageFragments {
		// Each fragment must start with fragment 0 and be marked as last

		if frag.Start.GetTaskFragmentNumber() != 0 {
			panic("Fragment start task number must be 0")
		}

		if !frag.LastFragment {
			panic("Fragment must be marked as last %s")
		}

		startTask := frag.Start.GetTaskNumber()
		endTask := frag.End.GetTaskNumber()

		totalTasks += int(endTask - startTask + 1)
	}

	assert.Equal(t, 320, totalTasks, "Total tasks processed should be 320")
}
