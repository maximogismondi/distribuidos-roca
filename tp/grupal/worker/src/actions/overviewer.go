package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof"

	// "github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof_handler"
	"github.com/cdipaolo/sentiment"
)

// Overviewer is a struct that implements the Action interface.
type Overviewer struct {
	model          sentiment.Models
	infraConfig    *model.InfraConfig
	itemHashFunc   func(workersCount int, item string) string
	randomHashFunc func(workersCount int) string
	eofHandler     *eof.StatelessEofHandler
}

// NewOverviewer creates a new Overviewer instance.
// It loads the sentiment model and initializes the worker count.
// If the model fails to load, it panics with an error message.
func NewOverviewer(infraConfig *model.InfraConfig) *Overviewer {
	model, err := sentiment.Restore()
	if err != nil {
		log.Panicf("Failed to load sentiment model: %s", err)
	}

	eofHandler := eof.NewStatelessEofHandler(
		infraConfig,
		overviewerNextStageData,
		utils.GetWorkerIdFromHash,
	)

	o := &Overviewer{
		model:          model,
		infraConfig:    infraConfig,
		itemHashFunc:   utils.GetWorkerIdFromHash,
		randomHashFunc: utils.RandomHash,
		eofHandler:     eofHandler,
	}

	return o
}

/*
muStage processes the input data and generates tasks for the next stage.
It analyzes the sentiment of the movie overview and creates Nu_1_Data tasks.

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"mapExchange": {
			"nu": {
				"": Task
			}
		}
	}
*/
func (o *Overviewer) muStage(data []*protocol.Mu_Data, clientId, creatorId string, taskNumber int) common.Tasks {
	tasks := make(common.Tasks)

	filteredData := utils.FilterSlice(data, func(input *protocol.Mu_Data) bool {
		return input.GetBudget() != 0 && input.GetRevenue() != 0 && input.GetOverview() != ""
	})

	mappedData := utils.MapSlice(filteredData, func(_ int, input *protocol.Mu_Data) *protocol.Nu_1_Data {
		return &protocol.Nu_1_Data{
			Id:        input.GetId(),
			Title:     input.GetTitle(),
			Revenue:   input.GetRevenue(),
			Budget:    input.GetBudget(),
			Sentiment: o.model.SentimentAnalysis(input.GetOverview(), sentiment.English).Score == 1,
		}
	})

	groupedData := utils.GroupByKey(mappedData, func(item *protocol.Nu_1_Data) string {
		return item.Id
	}, func(acc *protocol.Nu_1_Data, item *protocol.Nu_1_Data) {
		acc.Revenue += item.GetRevenue()
		acc.Budget += item.GetBudget()
	})

	nextStagesData, _ := o.nextStageData(common.MU_STAGE, clientId)

	taskDataCreator := func(stage string, data []*protocol.Nu_1_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Nu_1{
				Nu_1: &protocol.Nu_1{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	AddResultsToStateless(tasks, groupedData, nextStagesData[0], clientId, creatorId, taskNumber, taskDataCreator)

	return tasks
}

func (o *Overviewer) handleOmegaEOF(eofData *protocol.OmegaEOF_Data, clientId string) common.Tasks {
	tasks := make(common.Tasks)
	o.eofHandler.HandleOmegaEOF(tasks, eofData, clientId)

	return tasks
}

func (o *Overviewer) nextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	return overviewerNextStageData(stage, clientId, o.infraConfig, o.itemHashFunc)
}

func overviewerNextStageData(stage string, clientId string, infraConfig *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
	switch stage {
	case common.MU_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.NU_STAGE_1,
				Exchange:    infraConfig.GetMapExchange(),
				WorkerCount: infraConfig.GetMapCount(),
				RoutingKey:  infraConfig.GetBroadcastID(),
			},
		}, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return []common.NextStageData{}, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (o *Overviewer) Execute(task *protocol.Task) (common.Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()
	creatorId := task.GetTaskIdentifier().GetCreatorId()
	taskNumber := int(task.GetTaskIdentifier().GetTaskNumber())

	switch v := stage.(type) {
	case *protocol.Task_Mu:
		data := v.Mu.GetData()
		return o.muStage(data, clientId, creatorId, taskNumber), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return o.handleOmegaEOF(data, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}

func (m *Overviewer) LoadData(dirBase string) error {
	return nil
}

func (m *Overviewer) SaveData(task *protocol.Task) error {
	return nil
}

func (m *Overviewer) DeleteData(task *protocol.Task) error {
	return nil
}
