package actions

import (
	"fmt"
	"strconv"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof"
	// "github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof_handler"
)

// Mapper is a struct that implements the Action interface.
type Mapper struct {
	infraConfig    *model.InfraConfig
	itemHashFunc   func(workersCount int, itemId string) string
	randomHashFunc func(workersCount int) string
	eofHandler     *eof.StatelessEofHandler
}

// NewMapper creates a new Mapper instance.
// It initializes the worker count and returns a pointer to the Mapper struct.
func NewMapper(infraConfig *model.InfraConfig) *Mapper {
	eofHandler := eof.NewStatelessEofHandler(
		infraConfig,
		mapperNextStageData,
		utils.GetWorkerIdFromHash,
	)

	m := &Mapper{
		infraConfig:    infraConfig,
		itemHashFunc:   utils.GetWorkerIdFromHash,
		randomHashFunc: utils.RandomHash,
		eofHandler:     eofHandler,
	}

	return m
}

func (m *Mapper) delta1Stage(data []*protocol.Delta_1_Data, clientId, creatorId string, taskNumber int) common.Tasks {
	tasks := make(common.Tasks)

	mappedData := utils.MapSlice(data, func(_ int, input *protocol.Delta_1_Data) *protocol.Delta_2_Data {
		return &protocol.Delta_2_Data{
			Country:       input.GetCountry(),
			PartialBudget: input.GetBudget(),
		}
	})

	groupedData := utils.GroupByKey(mappedData, func(item *protocol.Delta_2_Data) string {
		return item.Country
	}, func(acc *protocol.Delta_2_Data, item *protocol.Delta_2_Data) {
		acc.PartialBudget += item.GetPartialBudget()
	})

	nextStagesData, _ := m.nextStageData(common.DELTA_STAGE_1, clientId)

	identifierFunc := func(input *protocol.Delta_2_Data) string {
		return input.Country + strconv.Itoa(int(input.PartialBudget))
	}

	taskDataCreator := func(stage string, data []*protocol.Delta_2_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Delta_2{
				Delta_2: &protocol.Delta_2{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	AddResultsToStateful(tasks, groupedData, nextStagesData[0], clientId, creatorId, taskNumber, m.itemHashFunc, identifierFunc, taskDataCreator, true)

	return tasks
}

func (m *Mapper) eta1Stage(data []*protocol.Eta_1_Data, clientId, creatorId string, taskNumber int) common.Tasks {
	tasks := make(common.Tasks)

	mappedData := utils.MapSlice(data, func(_ int, input *protocol.Eta_1_Data) *protocol.Eta_2_Data {
		return &protocol.Eta_2_Data{
			MovieId: input.GetMovieId(),
			Title:   input.GetTitle(),
			Rating:  float64(input.GetRating()),
			Count:   1,
		}
	})

	groupedData := utils.GroupByKey(mappedData, func(item *protocol.Eta_2_Data) string {
		return item.MovieId
	}, func(acc *protocol.Eta_2_Data, item *protocol.Eta_2_Data) {
		acc.Rating += item.GetRating()
		acc.Count += item.GetCount()
	})

	nextStagesData, _ := m.nextStageData(common.ETA_STAGE_1, clientId)

	identifierFunc := func(input *protocol.Eta_2_Data) string {
		return input.MovieId + strconv.Itoa(int(input.Rating)) + strconv.Itoa(int(input.Count))
	}

	taskDataCreator := func(stage string, data []*protocol.Eta_2_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Eta_2{
				Eta_2: &protocol.Eta_2{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	AddResultsToStateful(tasks, groupedData, nextStagesData[0], clientId, creatorId, taskNumber, m.itemHashFunc, identifierFunc, taskDataCreator, true)

	return tasks
}

/*
kappa1Stage partially counts actors participations in movies

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"reduceExchange": {
			"kappa_2": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (m *Mapper) kappa1Stage(data []*protocol.Kappa_1_Data, clientId, creatorId string, taskNumber int) common.Tasks {
	tasks := make(common.Tasks)

	mappedData := utils.MapSlice(data, func(_ int, input *protocol.Kappa_1_Data) *protocol.Kappa_2_Data {
		return &protocol.Kappa_2_Data{
			ActorId:               input.GetActorId(),
			ActorName:             input.GetActorName(),
			PartialParticipations: 1,
		}
	})

	groupedData := utils.GroupByKey(mappedData, func(item *protocol.Kappa_2_Data) string {
		return item.ActorId
	}, func(acc *protocol.Kappa_2_Data, item *protocol.Kappa_2_Data) {
		acc.PartialParticipations += item.GetPartialParticipations()
	})

	nextStagesData, _ := m.nextStageData(common.KAPPA_STAGE_1, clientId)

	identifierFunc := func(input *protocol.Kappa_2_Data) string {
		return input.ActorId + strconv.Itoa(int(input.PartialParticipations))
	}

	taskDataCreator := func(stage string, data []*protocol.Kappa_2_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Kappa_2{
				Kappa_2: &protocol.Kappa_2{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	AddResultsToStateful(tasks, groupedData, nextStagesData[0], clientId, creatorId, taskNumber, m.itemHashFunc, identifierFunc, taskDataCreator, true)

	return tasks
}

/*
nu1Stage partially counts and sum up revenue and budget from movies by sentiment.

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"reduceExchange": {
			"nu_2": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (m *Mapper) nu1Stage(data []*protocol.Nu_1_Data, clientId, creatorId string, taskNumber int) common.Tasks {
	tasks := make(common.Tasks)

	mappedData := utils.MapSlice(data, func(_ int, input *protocol.Nu_1_Data) *protocol.Nu_2_Data {
		return &protocol.Nu_2_Data{
			Sentiment: input.GetSentiment(),
			Ratio:     float32(float64(input.GetRevenue()) / float64(input.GetBudget())),
			Count:     1,
		}
	})

	groupedData := utils.GroupByKey(mappedData, func(item *protocol.Nu_2_Data) string {
		return fmt.Sprintf("%t", item.Sentiment)
	}, func(acc *protocol.Nu_2_Data, item *protocol.Nu_2_Data) {
		acc.Ratio += item.GetRatio()
		acc.Count += item.GetCount()
	})

	nextStagesData, _ := m.nextStageData(common.NU_STAGE_1, clientId)

	identifierFunc := func(input *protocol.Nu_2_Data) string {
		return strconv.FormatBool(input.Sentiment) + strconv.Itoa(int(input.Ratio)) + strconv.Itoa(int(input.Count))
	}

	taskDataCreator := func(stage string, data []*protocol.Nu_2_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Nu_2{
				Nu_2: &protocol.Nu_2{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	AddResultsToStateful(tasks, groupedData, nextStagesData[0], clientId, creatorId, taskNumber, m.itemHashFunc, identifierFunc, taskDataCreator, true)

	return tasks
}

func (m *Mapper) handleOmegaEOF(eofData *protocol.OmegaEOF_Data, clientId string) common.Tasks {
	tasks := make(common.Tasks)
	m.eofHandler.HandleOmegaEOF(tasks, eofData, clientId)

	return tasks
}

func (m *Mapper) nextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	return mapperNextStageData(stage, clientId, m.infraConfig, m.itemHashFunc)
}

func mapperNextStageData(stage string, clientId string, infraConfig *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
	routingKey := itemHashFunc(infraConfig.GetReduceCount(), clientId+stage)

	switch stage {
	case common.DELTA_STAGE_1:
		return []common.NextStageData{
			{
				Stage:       common.DELTA_STAGE_2,
				Exchange:    infraConfig.GetReduceExchange(),
				WorkerCount: infraConfig.GetReduceCount(),
				RoutingKey:  routingKey,
			},
		}, nil
	case common.ETA_STAGE_1:
		return []common.NextStageData{
			{
				Stage:       common.ETA_STAGE_2,
				Exchange:    infraConfig.GetReduceExchange(),
				WorkerCount: infraConfig.GetReduceCount(),
				RoutingKey:  routingKey,
			},
		}, nil
	case common.KAPPA_STAGE_1:
		return []common.NextStageData{
			{
				Stage:       common.KAPPA_STAGE_2,
				Exchange:    infraConfig.GetReduceExchange(),
				WorkerCount: infraConfig.GetReduceCount(),
				RoutingKey:  routingKey,
			},
		}, nil
	case common.NU_STAGE_1:
		return []common.NextStageData{
			{
				Stage:       common.NU_STAGE_2,
				Exchange:    infraConfig.GetReduceExchange(),
				WorkerCount: infraConfig.GetReduceCount(),
				RoutingKey:  routingKey,
			},
		}, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return []common.NextStageData{}, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (m *Mapper) Execute(task *protocol.Task) (common.Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()
	creatorId := task.GetTaskIdentifier().GetCreatorId()
	taskNumber := int(task.GetTaskIdentifier().GetTaskNumber())

	switch v := stage.(type) {
	case *protocol.Task_Delta_1:
		data := v.Delta_1.GetData()
		return m.delta1Stage(data, clientId, creatorId, taskNumber), nil

	case *protocol.Task_Eta_1:
		data := v.Eta_1.GetData()
		return m.eta1Stage(data, clientId, creatorId, taskNumber), nil

	case *protocol.Task_Kappa_1:
		data := v.Kappa_1.GetData()
		return m.kappa1Stage(data, clientId, creatorId, taskNumber), nil

	case *protocol.Task_Nu_1:
		data := v.Nu_1.GetData()
		return m.nu1Stage(data, clientId, creatorId, taskNumber), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return m.handleOmegaEOF(data, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}

func (m *Mapper) LoadData(dirBase string) error {
	return nil
}

func (m *Mapper) SaveData(task *protocol.Task) error {
	return nil
}

func (m *Mapper) DeleteData(task *protocol.Task) error {
	return nil
}
