package actions

import (
	"fmt"

	"slices"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof"
)

// Filter is a struct that implements the Action interface.
// It filters movies based on certain criteria.
// It is used in the worker to filter movies in the pipeline.
type Filter struct {
	infraConfig    *model.InfraConfig
	itemHashFunc   func(workersCount int, item string) string
	randomHashFunc func(workersCount int) string
	eofHandler     *eof.StatelessEofHandler
}

func NewFilter(infraConfig *model.InfraConfig) *Filter {
	eofHandler := eof.NewStatelessEofHandler(
		infraConfig,
		filterNextStageData,
		utils.GetWorkerIdFromHash,
	)

	f := &Filter{
		infraConfig:    infraConfig,
		itemHashFunc:   utils.GetWorkerIdFromHash,
		randomHashFunc: utils.RandomHash,
		eofHandler:     eofHandler,
	}

	return f
}

const ARGENTINA_COUNTRY string = "Argentina"
const SPAIN_COUNTRY string = "Spain"
const MOVIE_YEAR_2000 uint32 = 2000
const MOVIE_YEAR_2010 uint32 = 2010

func (f *Filter) alphaStage(data []*protocol.Alpha_Data, clientId, creatorId string, taskNumber int) common.Tasks {
	tasks := make(common.Tasks)

	filteredData := utils.FilterSlice(data, func(movie *protocol.Alpha_Data) bool {
		return movie.GetReleaseYear() >= MOVIE_YEAR_2000 && slices.Contains(movie.GetProdCountries(), ARGENTINA_COUNTRY)
	})

	resultsBeta := utils.MapSlice(filteredData, func(_ int, input *protocol.Alpha_Data) *protocol.Beta_Data {
		return &protocol.Beta_Data{
			Id:            input.GetId(),
			Title:         input.GetTitle(),
			ReleaseYear:   input.GetReleaseYear(),
			ProdCountries: input.GetProdCountries(),
			Genres:        input.GetGenres(),
		}
	})

	resultsZeta := utils.MapSlice(filteredData, func(_ int, input *protocol.Alpha_Data) *protocol.Zeta_Data {
		return &protocol.Zeta_Data{
			Data: &protocol.Zeta_Data_Movie_{
				Movie: &protocol.Zeta_Data_Movie{
					MovieId: input.GetId(),
					Title:   input.GetTitle(),
				},
			},
		}
	})

	resultsIota := utils.MapSlice(filteredData, func(_ int, input *protocol.Alpha_Data) *protocol.Iota_Data {
		return &protocol.Iota_Data{
			Data: &protocol.Iota_Data_Movie_{
				Movie: &protocol.Iota_Data_Movie{
					MovieId: input.GetId(),
				},
			},
		}
	})

	nextStagesData, _ := f.nextStageData(common.ALPHA_STAGE, clientId)

	nextStageDataBeta := nextStagesData[0]
	nextStageDataZeta := nextStagesData[1]
	nextStageDataIota := nextStagesData[2]

	identifierFuncZeta := func(item *protocol.Zeta_Data) string {
		return item.GetMovie().GetMovieId()
	}

	identifierFuncIota := func(item *protocol.Iota_Data) string {
		return item.GetMovie().GetMovieId()
	}

	hashFuncIota := func(workersCount int, item string) string {
		return f.infraConfig.GetBroadcastID()
	}

	hashFuncZeta := func(workersCount int, item string) string {
		return f.infraConfig.GetBroadcastID()
	}

	taskDataCreatorBeta := func(stage string, data []*protocol.Beta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Beta{
				Beta: &protocol.Beta{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	taskDataCreatorZeta := func(stage string, data []*protocol.Zeta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Zeta{
				Zeta: &protocol.Zeta{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
			TableType:      model.SMALL_TABLE,
		}
	}

	taskDataCreatorIota := func(stage string, data []*protocol.Iota_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Iota{
				Iota: &protocol.Iota{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
			TableType:      model.SMALL_TABLE,
		}
	}

	AddResultsToStateless(tasks, resultsBeta, nextStageDataBeta, clientId, creatorId, taskNumber, taskDataCreatorBeta)
	// Creo que son intercambiables, pero no estoy seguro
	// AddResultsToStateless(tasks, resultsZeta, nextStageDataZeta, clientId, creatorId, taskNumber, taskDataCreatorZeta)
	// AddResultsToStateless(tasks, resultsIota, nextStageDataIota, clientId, creatorId, taskNumber, taskDataCreatorIota)
	AddResultsToStateful(tasks, resultsZeta, nextStageDataZeta, clientId, creatorId, taskNumber, hashFuncIota, identifierFuncZeta, taskDataCreatorZeta, true)
	AddResultsToStateful(tasks, resultsIota, nextStageDataIota, clientId, creatorId, taskNumber, hashFuncZeta, identifierFuncIota, taskDataCreatorIota, true)

	return tasks
}

func (f *Filter) betaStage(data []*protocol.Beta_Data, clientId, creatorId string, taskNumber int) common.Tasks {
	tasks := make(common.Tasks)
	filteredData := utils.FilterSlice(data, func(movie *protocol.Beta_Data) bool {
		return movie.GetReleaseYear() < MOVIE_YEAR_2010 && slices.Contains(movie.GetProdCountries(), SPAIN_COUNTRY)
	},
	)

	results := utils.MapSlice(filteredData, func(_ int, input *protocol.Beta_Data) *protocol.Result1_Data {
		return &protocol.Result1_Data{
			Id:     input.GetId(),
			Title:  input.GetTitle(),
			Genres: input.GetGenres(),
		}
	})

	nextStagesData, _ := f.nextStageData(common.BETA_STAGE, clientId)

	taskDataCreator := func(stage string, data []*protocol.Result1_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Result1{
				Result1: &protocol.Result1{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	AddResultsToStateless(tasks, results, nextStagesData[0], clientId, creatorId, taskNumber, taskDataCreator)

	return tasks
}

func (f *Filter) gammaStage(data []*protocol.Gamma_Data, clientId, creatorId string, taskNumber int) common.Tasks {
	tasks := make(common.Tasks)

	filteredData := utils.FilterSlice(data, func(movie *protocol.Gamma_Data) bool {
		return len(movie.GetProdCountries()) == 1
	})

	results := utils.MapSlice(filteredData, func(_ int, input *protocol.Gamma_Data) *protocol.Delta_1_Data {
		return &protocol.Delta_1_Data{
			Country: input.GetProdCountries()[0],
			Budget:  input.GetBudget(),
		}
	})

	nextStagesData, _ := f.nextStageData(common.GAMMA_STAGE, clientId)

	taskDataCreator := func(stage string, data []*protocol.Delta_1_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Delta_1{
				Delta_1: &protocol.Delta_1{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	AddResultsToStateless(tasks, results, nextStagesData[0], clientId, creatorId, taskNumber, taskDataCreator)

	return tasks
}

func (f *Filter) handleOmegaEOF(eofData *protocol.OmegaEOF_Data, clientId string) common.Tasks {
	tasks := make(common.Tasks)
	f.eofHandler.HandleOmegaEOF(tasks, eofData, clientId)

	return tasks
}

func (f *Filter) nextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	return filterNextStageData(stage, clientId, f.infraConfig, f.itemHashFunc)
}

func filterNextStageData(stage string, clientId string, infraConfig *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
	switch stage {
	case common.ALPHA_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.BETA_STAGE,
				Exchange:    infraConfig.GetFilterExchange(),
				WorkerCount: infraConfig.GetFilterCount(),
				RoutingKey:  infraConfig.GetBroadcastID(),
			},
			{
				Stage:       common.ZETA_STAGE,
				Exchange:    infraConfig.GetJoinExchange(),
				WorkerCount: infraConfig.GetJoinCount(),
				RoutingKey:  infraConfig.GetBroadcastID(),
			},
			{
				Stage:       common.IOTA_STAGE,
				Exchange:    infraConfig.GetJoinExchange(),
				WorkerCount: infraConfig.GetJoinCount(),
				RoutingKey:  infraConfig.GetBroadcastID(),
			},
		}, nil

	case common.BETA_STAGE:
		return []common.NextStageData{
			{
				Stage:       model.RESULT_1_STAGE,
				Exchange:    infraConfig.GetResultExchange(),
				WorkerCount: 1,
				RoutingKey:  clientId,
			},
		}, nil

	case common.GAMMA_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.DELTA_STAGE_1,
				Exchange:    infraConfig.GetMapExchange(),
				WorkerCount: infraConfig.GetMapCount(),
				RoutingKey:  infraConfig.GetBroadcastID(),
			},
		}, nil

	default:
		log.Errorf("Invalid stage: %s", stage)
		return nil, fmt.Errorf("invalid stage: %s", stage)
	}
}

// Execute executes the action.
// It returns a map of tasks for the next stages.
// It returns an error if the action fails.
func (f *Filter) Execute(task *protocol.Task) (common.Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()
	creatorId := task.GetTaskIdentifier().GetCreatorId()
	taskNumber := int(task.GetTaskIdentifier().GetTaskNumber())

	switch v := stage.(type) {
	case *protocol.Task_Alpha:
		data := v.Alpha.GetData()
		return f.alphaStage(data, clientId, creatorId, taskNumber), nil

	case *protocol.Task_Beta:
		data := v.Beta.GetData()
		return f.betaStage(data, clientId, creatorId, taskNumber), nil

	case *protocol.Task_Gamma:
		data := v.Gamma.GetData()
		return f.gammaStage(data, clientId, creatorId, taskNumber), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return f.handleOmegaEOF(data, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}

func (m *Filter) LoadData(dirBase string) error {
	return nil
}
func (m *Filter) SaveData(task *protocol.Task) error {
	return nil
}
func (m *Filter) DeleteData(task *protocol.Task) error {
	return nil
}
