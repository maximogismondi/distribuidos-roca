package actions

import (
	"testing"

	"os"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	c "github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	topkheap "github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils/topkheap"
	"github.com/stretchr/testify/assert"
)

const HARDCODED_WORKER_ID = "0"
const CLIENT_ID = "testClientId"

func TestTopperExecute(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "test_serialization")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	var testInfraConfig = model.NewInfraConfig(
		HARDCODED_WORKER_ID,
		&model.WorkerClusterConfig{},
		&model.RabbitConfig{
			ResultExchange: "resultExchange",
			TopExchange:    "topExchange",
		},
		tempDir,
		0,
	)

	t.Run("Test Epsilon Stage with single task", func(t *testing.T) {

		var testTopper = NewTopper(testInfraConfig)

		task := &protocol.Task{
			ClientId: CLIENT_ID,
			Stage: &protocol.Task_Epsilon{
				Epsilon: &protocol.Epsilon{
					Data: []*protocol.Epsilon_Data{
						{ProdCountry: "Spain", TotalInvestment: 700},
						{ProdCountry: "Germany", TotalInvestment: 800},
						{ProdCountry: "Italy", TotalInvestment: 600},
						{ProdCountry: "Poland", TotalInvestment: 900},
						{ProdCountry: "USA", TotalInvestment: 1000},
					},
				},
			},
		}

		_, err := testTopper.Execute(task)
		assert.NoError(t, err, "Expected no error during execution")

		taskToProcess := make(c.Tasks)
		testTopper.epsilonResultStage(taskToProcess, CLIENT_ID)

		resultTask := taskToProcess[testInfraConfig.GetResultExchange()][CLIENT_ID][0]
		resultData := resultTask.GetResult2().GetData()

		assert.Len(t, resultData, 5, "Expected 5 countries in the result")
		assert.Equal(t, "USA", resultData[0].GetCountry(), "Expected USA to have the highest investment")
		assert.Equal(t, "Italy", resultData[4].GetCountry(), "Expected Argentina to have the lowest investment")
	})

	t.Run("Test Epsilon Stage with multiple tasks", func(t *testing.T) {

		var testTopper = NewTopper(testInfraConfig)

		task1 := &protocol.Task{
			ClientId: CLIENT_ID,
			Stage: &protocol.Task_Epsilon{
				Epsilon: &protocol.Epsilon{
					Data: []*protocol.Epsilon_Data{
						{ProdCountry: "Uruguay", TotalInvestment: 100},
						{ProdCountry: "Argentina", TotalInvestment: 200},
						{ProdCountry: "USA", TotalInvestment: 800},
						{ProdCountry: "Italy", TotalInvestment: 400},
					},
				},
			},
		}

		task2 := &protocol.Task{
			ClientId: CLIENT_ID,
			Stage: &protocol.Task_Epsilon{
				Epsilon: &protocol.Epsilon{
					Data: []*protocol.Epsilon_Data{
						{ProdCountry: "Brazil", TotalInvestment: 500},
						{ProdCountry: "Spain", TotalInvestment: 300},
					},
				},
			},
		}

		_, err := testTopper.Execute(task1)
		assert.NoError(t, err, "Expected no error during first execution")

		_, err = testTopper.Execute(task2)
		assert.NoError(t, err, "Expected no error during second execution")

		taskToProcess := make(c.Tasks)
		testTopper.epsilonResultStage(taskToProcess, CLIENT_ID)

		resultTask := taskToProcess[testInfraConfig.GetResultExchange()][CLIENT_ID][0]
		resultData := resultTask.GetResult2().GetData()

		assert.Len(t, resultData, 5, "Expected 5 countries in the result")
		assert.Equal(t, "USA", resultData[0].GetCountry(), "Expected USA to have the highest investment")
		assert.Equal(t, "Argentina", resultData[4].GetCountry(), "Expected Argentina to have the lowest investment")
		assert.Equal(t, uint64(200), resultData[4].GetTotalInvestment(), "Expected Argentina to have highest investment")

	})

	t.Run("Test Theta Stage with single task", func(t *testing.T) {

		var testTopper = NewTopper(testInfraConfig)

		task := &protocol.Task{
			ClientId: CLIENT_ID,
			Stage: &protocol.Task_Theta{
				Theta: &protocol.Theta{
					Data: []*protocol.Theta_Data{
						{Id: "1", Title: "Movie A", AvgRating: 4},
						{Id: "2", Title: "Movie B", AvgRating: 3},
						{Id: "3", Title: "Movie C", AvgRating: 5},
					},
				},
			},
		}

		_, err := testTopper.Execute(task)
		assert.NoError(t, err, "Expected no error during execution")

		taskToProcess := make(c.Tasks)
		testTopper.thetaResultStage(taskToProcess, CLIENT_ID)

		resultTask := taskToProcess[testInfraConfig.GetResultExchange()][CLIENT_ID][0]
		resultData := resultTask.GetResult3().GetData()

		assert.Len(t, resultData, 2, "Expected 2 movies in the result (highest and lowest)")
		assert.Equal(t, "Movie C", resultData[0].GetTitle(), "Expected Movie C to have the highest rating")
		assert.Equal(t, "Max", resultData[0].GetType(), "Expected Movie C to be of type Max")
		assert.Equal(t, float32(5), resultData[0].GetRating(), "Expected Movie C to have the highest rating")
		assert.Equal(t, "Movie B", resultData[1].GetTitle(), "Expected Movie B to have the lowest rating")
		assert.Equal(t, "Min", resultData[1].GetType(), "Expected Movie B to have the correct ID")
		assert.Equal(t, float32(3), resultData[1].GetRating(), "Expected Movie B to have the lowest rating")
	})

	t.Run("Test Theta Stage with multiple tasks", func(t *testing.T) {

		var testTopper = NewTopper(testInfraConfig)

		task1 := &protocol.Task{
			ClientId: CLIENT_ID,
			Stage: &protocol.Task_Theta{
				Theta: &protocol.Theta{
					Data: []*protocol.Theta_Data{
						{Id: "1", Title: "Movie A", AvgRating: 10},
						{Id: "2", Title: "Movie B", AvgRating: 4},
					},
				},
			},
		}

		task2 := &protocol.Task{
			ClientId: CLIENT_ID,
			Stage: &protocol.Task_Theta{
				Theta: &protocol.Theta{
					Data: []*protocol.Theta_Data{
						{Id: "3", Title: "Movie C", AvgRating: 2},
						{Id: "2", Title: "Movie B", AvgRating: 5},
					},
				},
			},
		}

		_, err := testTopper.Execute(task1)
		assert.NoError(t, err, "Expected no error during first execution")

		_, err = testTopper.Execute(task2)
		assert.NoError(t, err, "Expected no error during second execution")

		taskToProcess := make(c.Tasks)
		testTopper.thetaResultStage(taskToProcess, CLIENT_ID)
		resultTask := taskToProcess[testInfraConfig.GetResultExchange()][CLIENT_ID][0]
		resultData := resultTask.GetResult3().GetData()

		assert.Len(t, resultData, 2, "Expected 2 movies in the result (highest and lowest)")
		assert.Equal(t, "Movie A", resultData[0].GetTitle(), "Expected Movie A to have the highest rating")
		assert.Equal(t, "Max", resultData[0].GetType(), "Expected Movie A to be of type Max")
		assert.Equal(t, float32(10), resultData[0].GetRating(), "Expected Movie A to have the highest rating")
		assert.Equal(t, "Movie C", resultData[1].GetTitle(), "Expected Movie C to have the lowest rating")
		assert.Equal(t, "Min", resultData[1].GetType(), "Expected Movie C to have the correct ID")
		assert.Equal(t, float32(2), resultData[1].GetRating(), "Expected Movie C to have the lowest rating")

	})

	t.Run("Test Lambda Stage with single task", func(t *testing.T) {
		var testTopper = NewTopper(testInfraConfig)

		task := &protocol.Task{
			ClientId: CLIENT_ID,
			Stage: &protocol.Task_Lambda{
				Lambda: &protocol.Lambda{
					Data: []*protocol.Lambda_Data{
						{ActorId: "1", ActorName: "Actor A", Participations: 3},
						{ActorId: "2", ActorName: "Actor B", Participations: 2},
						{ActorId: "3", ActorName: "Actor C", Participations: 4},
						{ActorId: "4", ActorName: "Actor D", Participations: 5},
						{ActorId: "5", ActorName: "Actor E", Participations: 6},
						{ActorId: "6", ActorName: "Actor F", Participations: 7},
						{ActorId: "7", ActorName: "Actor G", Participations: 8},
						{ActorId: "8", ActorName: "Actor H", Participations: 9},
						{ActorId: "9", ActorName: "Actor I", Participations: 10},
						{ActorId: "10", ActorName: "Actor J", Participations: 11},
						{ActorId: "11", ActorName: "Actor K", Participations: 12},
						{ActorId: "12", ActorName: "Actor L", Participations: 13},
					},
				},
			},
		}

		_, err := testTopper.Execute(task)
		assert.NoError(t, err, "Expected no error during execution")

		taskToProcess := make(c.Tasks)
		testTopper.lambdaResultStage(taskToProcess, CLIENT_ID)

		resultTask := taskToProcess[testInfraConfig.GetResultExchange()][CLIENT_ID][0]
		resultData := resultTask.GetResult4().GetData()

		assert.Len(t, resultData, 10, "Expected 10 actors in the result")
		assert.Equal(t, "Actor L", resultData[0].GetActorName(), "Expected Actor L to have the highest participations")
		assert.Equal(t, "12", resultData[0].GetActorId(), "Expected Actor L to have the highest participations")

		assert.Equal(t, "Actor C", resultData[9].GetActorName(), "Expected Actor C to have the lowest participations")
		assert.Equal(t, "3", resultData[9].GetActorId(), "Expected Actor C to have the lowest participations")

		for _, data := range resultData {
			assert.NotEqual(t, "Actor A", data.GetActorName(), "Actor A should not be in the result")
			assert.NotEqual(t, "Actor B", data.GetActorName(), "Actor B should not be in the result")
		}
	})

	t.Run("Test Lambda Stage with multiple tasks", func(t *testing.T) {
		var testTopper = NewTopper(testInfraConfig)

		task := &protocol.Task{
			ClientId: CLIENT_ID,
			Stage: &protocol.Task_Lambda{
				Lambda: &protocol.Lambda{
					Data: []*protocol.Lambda_Data{
						{ActorId: "1", ActorName: "Actor A", Participations: 3},
						{ActorId: "2", ActorName: "Actor B", Participations: 2},
						{ActorId: "3", ActorName: "Actor C", Participations: 4},
						{ActorId: "4", ActorName: "Actor D", Participations: 5},
						{ActorId: "5", ActorName: "Actor E", Participations: 6},
						{ActorId: "6", ActorName: "Actor F", Participations: 7},
						{ActorId: "7", ActorName: "Actor G", Participations: 8},
						{ActorId: "8", ActorName: "Actor H", Participations: 9},
					},
				},
			},
		}

		task2 := &protocol.Task{
			ClientId: CLIENT_ID,
			Stage: &protocol.Task_Lambda{
				Lambda: &protocol.Lambda{
					Data: []*protocol.Lambda_Data{
						{ActorId: "9", ActorName: "Actor I", Participations: 10},
						{ActorId: "10", ActorName: "Actor J", Participations: 11},
						{ActorId: "11", ActorName: "Actor K", Participations: 12},
						{ActorId: "12", ActorName: "Actor L", Participations: 13},
					},
				},
			},
		}

		_, err := testTopper.Execute(task)
		assert.NoError(t, err, "Expected no error during first execution")

		_, err = testTopper.Execute(task2)
		assert.NoError(t, err, "Expected no error during second execution")

		taskToProcess := make(c.Tasks)
		testTopper.lambdaResultStage(taskToProcess, CLIENT_ID)

		resultTask := taskToProcess[testInfraConfig.GetResultExchange()][CLIENT_ID][0]
		resultData := resultTask.GetResult4().GetData()

		assert.Len(t, resultData, 10, "Expected 10 actors in the result")
		assert.Equal(t, "Actor L", resultData[0].GetActorName(), "Expected Actor L to have the highest participations")
		assert.Equal(t, "12", resultData[0].GetActorId(), "Expected Actor L to have the highest participations")

		assert.Equal(t, "Actor C", resultData[9].GetActorName(), "Expected Actor C to have the lowest participations")
		assert.Equal(t, "3", resultData[9].GetActorId(), "Expected Actor C to have the lowest participations")
	})

}

func TestResultStagesWithEmptyTasks(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "test_serialization")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	var testInfraConfig = model.NewInfraConfig(
		HARDCODED_WORKER_ID,
		&model.WorkerClusterConfig{},
		&model.RabbitConfig{
			ResultExchange: "resultExchange",
			TopExchange:    "topExchange",
		},
		tempDir,
		0,
	)

	var testTopper = NewTopper(testInfraConfig)

	t.Run("Test Epsilon Result Stage with empty tasks", func(t *testing.T) {
		emptyTask := &protocol.Task{
			ClientId: CLIENT_ID,
			Stage: &protocol.Task_Epsilon{
				Epsilon: &protocol.Epsilon{
					Data: []*protocol.Epsilon_Data{},
				},
			},
		}

		_, err := testTopper.Execute(emptyTask)
		assert.NoError(t, err, "Expected no error during execution")

		taskToProcess := make(c.Tasks)
		testTopper.epsilonResultStage(taskToProcess, CLIENT_ID)

		resultTask := taskToProcess[testInfraConfig.GetResultExchange()][CLIENT_ID][0]
		resultData := resultTask.GetResult4().GetData()

		assert.Len(t, resultData, 0, "Expected no tasks to be created for empty input")
	})

	t.Run("Test Lambda Result Stage with empty tasks", func(t *testing.T) {

		var testTopper = NewTopper(testInfraConfig)

		emptyTask := &protocol.Task{
			ClientId: CLIENT_ID,
			Stage: &protocol.Task_Lambda{
				Lambda: &protocol.Lambda{
					Data: []*protocol.Lambda_Data{},
				},
			},
		}

		_, err := testTopper.Execute(emptyTask)
		assert.NoError(t, err, "Expected no error during execution")

		taskToProcess := make(c.Tasks)
		testTopper.lambdaResultStage(taskToProcess, CLIENT_ID)

		resultTask := taskToProcess[testInfraConfig.GetResultExchange()][CLIENT_ID][0]
		resultData := resultTask.GetResult4().GetData()

		assert.Len(t, resultData, 0, "Expected no tasks to be created for empty input")

	})

	t.Run("Test Theta Result Stage with empty tasks", func(t *testing.T) {

		emptyTask := &protocol.Task{
			ClientId: CLIENT_ID,
			Stage: &protocol.Task_Theta{
				Theta: &protocol.Theta{
					Data: []*protocol.Theta_Data{},
				},
			},
		}

		_, err := testTopper.Execute(emptyTask)
		assert.NoError(t, err, "Expected no error during execution")

		taskToProcess := make(c.Tasks)
		testTopper.thetaResultStage(taskToProcess, CLIENT_ID)

		resultTask := taskToProcess[testInfraConfig.GetResultExchange()][CLIENT_ID][0]
		resultData := resultTask.GetResult3().GetData()

		assert.Len(t, resultData, 0, "Expected no tasks to be created for empty input")

	})
}

// func TestEOFArrival(t *testing.T) {

// tempDir, err := os.MkdirTemp("", "test_serialization")
// if err != nil {
// 	t.Fatalf("Failed to create temp directory: %v", err)
// }
// defer os.RemoveAll(tempDir)

// var testInfraConfig = model.NewInfraConfig(
// 	HARDCODED_WORKER_ID,
// 	&model.WorkerClusterConfig{},
// 	&model.RabbitConfig{
// 		ResultExchange: "resultExchange",
// 		TopExchange:    "topExchange",
// 	},
// 	tempDir,
// )

// var testTopper = NewTopper(testInfraConfig)
// t.Run("EOF arrival bring result and EOF Message", func(t *testing.T) {
// 	var testTopper = NewTopper(testInfraConfig)
// 	t.Run("EOF arrival bring result and EOF Message", func(t *testing.T) {

// 		epsilonTask := &protocol.Task{
// 			ClientId: CLIENT_ID,
// 			Stage: &protocol.Task_Epsilon{
// 				Epsilon: &protocol.Epsilon{
// 					Data: []*protocol.Epsilon_Data{
// 						{ProdCountry: "Spain", TotalInvestment: 700},
// 						{ProdCountry: "Germany", TotalInvestment: 800},
// 						{ProdCountry: "Italy", TotalInvestment: 600},
// 						{ProdCountry: "Poland", TotalInvestment: 900},
// 						{ProdCountry: "USA", TotalInvestment: 1000},
// 					},
// 				},
// 			},
// 		}

// 		eofTask := &protocol.Task{
// 			ClientId: CLIENT_ID,
// 			Stage: &protocol.Task_OmegaEOF{
// 				OmegaEOF: &protocol.OmegaEOF{
// 					Data: &protocol.OmegaEOF_Data{
// 						Stage: EPSILON_STAGE,
// 					},
// 				},
// 			},
// 		}

// 		_, err := testTopper.Execute(epsilonTask)
// 		tasks, err := testTopper.Execute(eofTask)
// 		assert.NoError(t, err, "Expected no error during execution")

// 		resultTask := tasks[testInfraConfig.GetResultExchange()][CLIENT_ID][0]
// 		resultData := resultTask.GetResult2().GetData()
// 		circularEofTask := tasks[testInfraConfig.GetTopExchange()][EPSILON_STAGE][testInfraConfig.GetNodeId()]
// 		resultEofTask := tasks[testInfraConfig.GetResultExchange()][CLIENT_ID][0]

// 		assert.Len(t, resultData, 5, "Expected 5 results for EOF arrival")
// 		assert.NotNil(t, circularEofTask, "Expected EOF task to be present in the tasks")
// 		assert.NotNil(t, resultEofTask, "Expected EOF task to be present in the tasks")
// 	})

// }

func TestHeap(t *testing.T) {
	{
		h := topkheap.NewTopKMaxHeap[int, string](3)
		h.Insert(10, "A")
		h.Insert(20, "B")
		h.Insert(5, "C")
		h.Insert(40, "D")
		h.Insert(15, "E")

		top := h.GetTopK()
		assert.Equal(t, []string{"D", "B", "E"}, top)
	}

	{
		h := topkheap.NewTopKMinHeap[int, string](3)
		h.Insert(10, "A")
		h.Insert(20, "B")
		h.Insert(5, "C")
		h.Insert(40, "D")
		h.Insert(15, "E")

		top := h.GetTopK()
		assert.Equal(t, []string{"C", "A", "E"}, top)
	}
}
