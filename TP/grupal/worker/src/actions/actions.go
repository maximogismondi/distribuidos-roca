package actions

import (
	"slices"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/op/go-logging"
	"google.golang.org/protobuf/proto"
)

var log = logging.MustGetLogger("log")

func NewPartialData[T proto.Message]() *common.PartialData[T] {
	return &common.PartialData[T]{
		Data:           make(map[string]T),
		TaskFragments:  make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
		OmegaProcessed: false,
		RingRound:      0,
		IsReady:        false,
	}
}

type Action interface {
	// Execute executes the action.
	// It returns a map of tasks for the next stages.
	// It returns an error if the action fails.
	Execute(task *protocol.Task) (common.Tasks, error)
	SaveData(task *protocol.Task) error
	LoadData(dirBase string) error
	DeleteData(task *protocol.Task) error
}

// NewAction creates a new action based on the worker type.
func NewAction(workerType string, infraConfig *model.InfraConfig) Action {
	kind := model.ActionType(workerType)

	switch kind {
	case model.FilterAction:
		return NewFilter(infraConfig)
	case model.OverviewerAction:
		return NewOverviewer(infraConfig)
	case model.MapperAction:
		return NewMapper(infraConfig)
	case model.JoinerAction:
		return NewJoiner(infraConfig)
	case model.ReducerAction:
		return NewReducer(infraConfig)
	case model.MergerAction:
		return NewMerger(infraConfig)
	case model.TopperAction:
		return NewTopper(infraConfig)
	default:
		log.Panicf("Unknown worker type: %s", workerType)
		return nil
	}
}

func AddResultsToStateless[T any](
	tasks common.Tasks,
	results []T,
	nextStageData common.NextStageData,
	clientId string,
	creatorId string,
	taskNumber int,
	taskDataCreator func(stage string, data []T, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task,
) {
	exchange := nextStageData.Exchange
	routingKey := nextStageData.RoutingKey
	stage := nextStageData.Stage

	if _, ok := tasks[exchange]; !ok {
		tasks[exchange] = make(map[string][]*protocol.Task)
	}

	taskIdentifier := &protocol.TaskIdentifier{
		CreatorId:          creatorId,
		TaskNumber:         uint32(taskNumber),
		TaskFragmentNumber: 0,
		LastFragment:       true,
	}

	task := taskDataCreator(
		stage,
		results,
		clientId,
		taskIdentifier,
	)

	if _, ok := tasks[exchange][routingKey]; !ok {
		tasks[exchange][routingKey] = []*protocol.Task{}
	}

	tasks[exchange][routingKey] = append(tasks[exchange][routingKey], task)
}

func AddResultsToStateful[T any](
	tasks common.Tasks,
	results []T,
	nextStageData common.NextStageData,
	clientId string,
	creatorId string,
	taskNumber int,
	hashFunc func(workersCount int, input string) string,
	identifierFunc func(input T) string,
	taskDataCreator func(stage string, data []T, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task,
	alwaysAddTasks bool,
) {
	exchange := nextStageData.Exchange
	workerCount := nextStageData.WorkerCount
	stage := nextStageData.Stage

	if _, ok := tasks[exchange]; !ok {
		tasks[exchange] = make(map[string][]*protocol.Task)
	}

	dataByRK := make(map[string][]T)

	for _, item := range results {
		routingKey := hashFunc(workerCount, identifierFunc(item))
		dataByRK[routingKey] = append(dataByRK[routingKey], item)
	}

	destinationNodes := utils.MapKeys(dataByRK)

	slices.Sort(destinationNodes)

	if len(destinationNodes) == 0 && alwaysAddTasks {
		routingKey := hashFunc(workerCount, creatorId+stage)
		destinationNodes = append(destinationNodes, routingKey)
	}

	for index, routingKey := range destinationNodes {
		taskIdentifier := &protocol.TaskIdentifier{
			CreatorId:          creatorId,
			TaskNumber:         uint32(taskNumber),
			TaskFragmentNumber: uint32(index),
			LastFragment:       index == len(destinationNodes)-1,
		}

		data := []T{}

		if _, ok := dataByRK[routingKey]; ok {
			data = dataByRK[routingKey]
		}

		task := taskDataCreator(
			stage,
			data,
			clientId,
			taskIdentifier,
		)

		if _, ok := tasks[exchange][routingKey]; !ok {
			tasks[exchange][routingKey] = []*protocol.Task{}
		}

		tasks[exchange][routingKey] = append(tasks[exchange][routingKey], task)
	}
}

func ProcessStage[T proto.Message](
	partial *common.PartialData[T],
	newItems []T,
	clientID string,
	taskIdentifier *protocol.TaskIdentifier,
	merge func(T, T),
	keySelector func(T) string,
	infraConfig *model.InfraConfig,
	stage string,

) {

	taskID := model.TaskFragmentIdentifier{
		CreatorId:          taskIdentifier.GetCreatorId(),
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	if partial == nil {
		log.Debugf("Partial data is nil, creating new partial data for task %s",
			taskID)
		partial = NewPartialData[T]()
	}
	if partial.IsReady {
		log.Debugf("Partial data is ready, skipping task %s", taskID)
		return
	}

	if _, processed := partial.TaskFragments[taskID]; processed {
		// Task already handled
		log.Debugf("Task %s already processed, skipping", taskID)
		return
	}

	// Mark task as processed
	partial.TaskFragments[taskID] = common.FragmentStatus{Logged: false}

	// Aggregate data
	utils.MergeIntoMap(partial.Data, newItems, keySelector, merge)
}
