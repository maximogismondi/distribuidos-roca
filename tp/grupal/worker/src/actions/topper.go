package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils/storage"
	topkheap "github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils/topkheap"
	"google.golang.org/protobuf/proto"
)

const EPSILON_TOP_K = 5
const LAMBDA_TOP_K = 10
const THETA_TOP_K = 1

const TOPPER_STAGES_COUNT uint = 3

// ParcilResult is a struct that holds the results of the different stages.

func newTopperPartialData[K topkheap.Ordered, V any](top_k int, max bool) *common.TopperPartialData[K, V] {
	var heapInstance topkheap.TopKHeap[K, V]
	if max {
		heapInstance = topkheap.NewTopKMaxHeap[K, V](top_k)
	} else {
		heapInstance = topkheap.NewTopKMinHeap[K, V](top_k)
	}

	return &common.TopperPartialData[K, V]{
		Heap:           heapInstance,
		TaskFragments:  make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
		OmegaProcessed: false,
		RingRound:      0,
	}
}

func NewTopperPartialResults() *common.TopperPartialResults {
	return &common.TopperPartialResults{
		EpsilonData: newTopperPartialData[uint64, *protocol.Epsilon_Data](EPSILON_TOP_K, true),
		ThetaData: &common.ThetaPartialData{
			MinPartialData: newTopperPartialData[float32, *protocol.Theta_Data](THETA_TOP_K, false),
			MaxPartialData: newTopperPartialData[float32, *protocol.Theta_Data](THETA_TOP_K, true),
		},
		LamdaData: newTopperPartialData[uint64, *protocol.Lambda_Data](LAMBDA_TOP_K, true),
	}
}

// Topper is a struct that implements the Action interface.
type Topper struct {
	infraConfig    *model.InfraConfig
	partialResults map[string]*common.TopperPartialResults
	itemHashFunc   func(workersCount int, item string) string
	eofHandler     *eof.StatefulEofHandler
}

func (t *Topper) makePartialResults(clientId string) {
	if _, ok := t.partialResults[clientId]; ok {
		return
	}

	t.partialResults[clientId] = NewTopperPartialResults()
}

// NewTopper creates a new Topper instance.
// It initializes the worker count and returns a pointer to the Topper struct.
func NewTopper(infraConfig *model.InfraConfig) *Topper {
	eofHandler := eof.NewStatefulEofHandler(
		model.TopperAction,
		infraConfig,
		topperNextStageData,
		utils.GetWorkerIdFromHash,
	)

	topper := &Topper{
		infraConfig:    infraConfig,
		partialResults: make(map[string]*common.TopperPartialResults),
		itemHashFunc:   utils.GetWorkerIdFromHash,
		eofHandler:     eofHandler,
	}

	go storage.StartCleanupRoutine(infraConfig.GetDirectory(), infraConfig.GetCleanUpTime())

	return topper
}

func (t *Topper) epsilonStage(data []*protocol.Epsilon_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	partialData := t.partialResults[clientId].EpsilonData

	valueFunc := func(input *protocol.Epsilon_Data) uint64 {
		return input.GetTotalInvestment()
	}

	processTopperStage(
		partialData,
		data,
		taskIdentifier,
		valueFunc,
	)

	return nil
}

func (t *Topper) thetaStage(data []*protocol.Theta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	partialData := t.partialResults[clientId].ThetaData
	minPartialData := partialData.MinPartialData
	maxPartialData := partialData.MaxPartialData

	minValueFunc := func(input *protocol.Theta_Data) float32 {
		return input.GetAvgRating()
	}

	maxValueFunc := func(input *protocol.Theta_Data) float32 {
		return input.GetAvgRating()
	}

	processTopperStage(
		minPartialData,
		data,
		taskIdentifier,
		minValueFunc,
	)

	processTopperStage(
		maxPartialData,
		data,
		taskIdentifier,
		maxValueFunc,
	)

	return nil
}

func (t *Topper) lambdaStage(data []*protocol.Lambda_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	partialData := t.partialResults[clientId].LamdaData

	valueFunc := func(input *protocol.Lambda_Data) uint64 {
		return input.GetParticipations()
	}

	processTopperStage(
		partialData,
		data,
		taskIdentifier,
		valueFunc,
	)

	return nil
}

func (t *Topper) epsilonResultStage(tasks common.Tasks, clientId string) {
	epsilonHeap := t.partialResults[clientId].EpsilonData.Heap
	partialData := epsilonHeap.GetTopK()

	results := utils.MapSlice(partialData, func(position int, data *protocol.Epsilon_Data) *protocol.Result2_Data {
		return &protocol.Result2_Data{
			Position:        uint32(position + 1),
			Country:         data.GetProdCountry(),
			TotalInvestment: data.GetTotalInvestment(),
		}
	})

	taskDataCreator := func(stage string, data []*protocol.Result2_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Result2{
				Result2: &protocol.Result2{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := t.getNextStageData(common.EPSILON_STAGE, clientId)

	creatorId := t.infraConfig.GetNodeId()

	AddResultsToStateless(tasks, results, nextStageData[0], clientId, creatorId, 0, taskDataCreator)
}

func (t *Topper) thetaResultStage(tasks common.Tasks, clientId string) {
	thetaData := t.partialResults[clientId].ThetaData

	minHeapData := thetaData.MinPartialData.Heap
	maxHeapData := thetaData.MaxPartialData.Heap

	minPartialData := minHeapData.GetTopK()
	maxPartialData := maxHeapData.GetTopK()

	minResults := utils.MapSlice(minPartialData, func(_ int, data *protocol.Theta_Data) *protocol.Result3_Data {
		return &protocol.Result3_Data{
			Type:   common.TYPE_MIN,
			Id:     data.GetId(),
			Title:  data.GetTitle(),
			Rating: data.GetAvgRating(),
		}
	})

	maxResults := utils.MapSlice(maxPartialData, func(_ int, data *protocol.Theta_Data) *protocol.Result3_Data {
		return &protocol.Result3_Data{
			Type:   common.TYPE_MAX,
			Id:     data.GetId(),
			Title:  data.GetTitle(),
			Rating: data.GetAvgRating(),
		}
	})

	results := make([]*protocol.Result3_Data, 0, len(minResults)+len(maxResults))
	results = append(results, maxResults...)
	results = append(results, minResults...)

	taskDataCreator := func(stage string, data []*protocol.Result3_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Result3{
				Result3: &protocol.Result3{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := t.getNextStageData(common.THETA_STAGE, clientId)

	creatorId := t.infraConfig.GetNodeId()

	AddResultsToStateless(tasks, results, nextStageData[0], clientId, creatorId, 0, taskDataCreator)
}

func (t *Topper) lambdaResultStage(tasks common.Tasks, clientId string) {
	lambdaHeap := t.partialResults[clientId].LamdaData.Heap
	partialData := lambdaHeap.GetTopK()

	results := utils.MapSlice(partialData, func(position int, data *protocol.Lambda_Data) *protocol.Result4_Data {
		return &protocol.Result4_Data{
			Position:       uint32(position + 1),
			ActorId:        data.GetActorId(),
			ActorName:      data.GetActorName(),
			Participations: data.GetParticipations(),
		}
	})

	taskDataCreator := func(stage string, data []*protocol.Result4_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Result4{
				Result4: &protocol.Result4{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := t.getNextStageData(common.LAMBDA_STAGE, clientId)

	creatorId := t.infraConfig.GetNodeId()

	AddResultsToStateless(tasks, results, nextStageData[0], clientId, creatorId, 0, taskDataCreator)
}

func (t *Topper) addResultsToNextStage(tasks common.Tasks, stage string, clientId string) error {
	switch stage {
	case common.EPSILON_STAGE:
		t.epsilonResultStage(tasks, clientId)
		// t.partialResults[clientId].epsilonData.Delete()
	case common.LAMBDA_STAGE:
		t.lambdaResultStage(tasks, clientId)
		// t.partialResults[clientId].lamdaData.Delete()
	case common.THETA_STAGE:
		t.thetaResultStage(tasks, clientId)
		// t.partialResults[clientId].thetaData.maxHeap.Delete()
		// t.partialResults[clientId].thetaData.minHeap.Delete()
	default:
		return fmt.Errorf("invalid stage: %s", stage)
	}

	return nil
}

func (t *Topper) getNextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	return topperNextStageData(stage, clientId, t.infraConfig, t.itemHashFunc)
}

func topperNextStageData(stage string, clientId string, infraConfig *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
	switch stage {
	case common.EPSILON_STAGE:
		return []common.NextStageData{
			{
				Stage:       model.RESULT_2_STAGE,
				Exchange:    infraConfig.GetResultExchange(),
				WorkerCount: 1,
				RoutingKey:  clientId,
			},
		}, nil
	case common.THETA_STAGE:
		return []common.NextStageData{
			{
				Stage:       model.RESULT_3_STAGE,
				Exchange:    infraConfig.GetResultExchange(),
				WorkerCount: 1,
				RoutingKey:  clientId,
			},
		}, nil
	case common.LAMBDA_STAGE:
		return []common.NextStageData{
			{
				Stage:       model.RESULT_4_STAGE,
				Exchange:    infraConfig.GetResultExchange(),
				WorkerCount: 1,
				RoutingKey:  clientId,
			},
		}, nil
	case common.RING_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.RING_STAGE,
				Exchange:    infraConfig.GetEofExchange(),
				WorkerCount: infraConfig.GetTopCount(),
				RoutingKey:  infraConfig.GetNodeId(),
			},
		}, nil
	default:
		return nil, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (t *Topper) omegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) common.Tasks {
	tasks := make(common.Tasks)

	omegaReady, err := t.getOmegaProcessed(clientId, data.GetStage())
	if err != nil {
		log.Errorf("Failed to get omega ready for stage %s: %s", data.GetStage(), err)
		return tasks
	}
	if omegaReady {
		log.Debugf("Omega EOF for stage %s has already been processed for client %s", data.GetStage(), clientId)
		return tasks
	}

	t.updateOmegaProcessed(clientId, data.GetStage())

	t.eofHandler.HandleOmegaEOF(tasks, data, clientId)

	return tasks
}

func (t *Topper) ringEOFStage(data *protocol.RingEOF, clientId string) common.Tasks {
	tasks := make(common.Tasks)

	taskIdentifiers, err := t.getTaskIdentifiers(clientId, data.GetStage())
	if err != nil {
		log.Errorf("Failed to get task identifiers for stage %s: %s", data.GetStage(), err)
		return tasks
	}

	ringRound, err := t.getRingRound(clientId, data.GetStage())
	if err != nil {
		log.Errorf("Failed to get ring round for stage %s: %s", data.GetStage(), err)
		return tasks
	}
	if ringRound >= data.GetRoundNumber() {
		log.Debugf("Ring EOF for stage %s and client %s has already been processed for round %d", data.GetStage(), clientId, ringRound)
		return tasks
	}

	t.updateRingRound(clientId, data.GetStage(), data.GetRoundNumber())

	taskCount := t.participatesInResults(clientId, data.GetStage())
	ready := t.eofHandler.HandleRingEOF(tasks, data, clientId, taskIdentifiers, taskCount)

	if ready && taskCount > 0 {
		err = t.addResultsToNextStage(tasks, data.GetStage(), clientId)
		if err != nil {
			log.Errorf("Failed to add results to next stage for stage %s: %s", data.GetStage(), err)
			return nil
		}
		err := t.setToDelete(clientId, data.GetStage())
		if err != nil {
			log.Errorf("Failed to delete stage %s for client %s: %s", data.GetStage(), clientId, err)
		}
	}

	return tasks
}

func (t *Topper) Execute(task *protocol.Task) (common.Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()
	taskIdentifier := task.GetTaskIdentifier()

	t.makePartialResults(clientId)

	switch v := stage.(type) {
	case *protocol.Task_Epsilon:
		data := v.Epsilon.GetData()
		return t.epsilonStage(data, clientId, taskIdentifier), nil

	case *protocol.Task_Theta:
		data := v.Theta.GetData()
		return t.thetaStage(data, clientId, taskIdentifier), nil

	case *protocol.Task_Lambda:
		data := v.Lambda.GetData()
		return t.lambdaStage(data, clientId, taskIdentifier), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return t.omegaEOFStage(data, clientId), nil

	case *protocol.Task_RingEOF:
		return t.ringEOFStage(v.RingEOF, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}

func (t *Topper) getOmegaProcessed(clientId string, stage string) (bool, error) {
	partialResults := t.partialResults[clientId]
	switch stage {
	case common.EPSILON_STAGE:
		return partialResults.EpsilonData.OmegaProcessed, nil
	case common.THETA_STAGE:
		return partialResults.ThetaData.MinPartialData.OmegaProcessed, nil
	case common.LAMBDA_STAGE:
		return partialResults.LamdaData.OmegaProcessed, nil
	default:
		return false, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (t *Topper) getRingRound(clientId string, stage string) (uint32, error) {
	partialResults := t.partialResults[clientId]
	switch stage {
	case common.EPSILON_STAGE:
		return partialResults.EpsilonData.RingRound, nil
	case common.THETA_STAGE:
		return partialResults.ThetaData.MinPartialData.RingRound, nil
	case common.LAMBDA_STAGE:
		return partialResults.LamdaData.RingRound, nil
	default:
		return 0, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (t *Topper) getTaskIdentifiers(clientId string, stage string) ([]model.TaskFragmentIdentifier, error) {
	partialResults := t.partialResults[clientId]
	switch stage {
	case common.EPSILON_STAGE:
		return utils.MapKeys(partialResults.EpsilonData.TaskFragments), nil
	case common.THETA_STAGE:
		return utils.MapKeys(partialResults.ThetaData.MinPartialData.TaskFragments), nil
	case common.LAMBDA_STAGE:
		return utils.MapKeys(partialResults.LamdaData.TaskFragments), nil
	default:
		return nil, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (t *Topper) participatesInResults(clientId string, stage string) int {
	partialResults, ok := t.partialResults[clientId]
	if !ok {
		return 0
	}

	participates := false

	switch stage {
	case common.EPSILON_STAGE:
		participates = partialResults.EpsilonData.Heap.Len() > 0
	case common.THETA_STAGE:
		participates = partialResults.ThetaData.MinPartialData.Heap.Len() > 0 ||
			partialResults.ThetaData.MaxPartialData.Heap.Len() > 0
	case common.LAMBDA_STAGE:
		participates = partialResults.LamdaData.Heap.Len() > 0
	default:
		log.Errorf("Invalid stage: %s", stage)
		return 0
	}

	if participates {
		return 1
	}
	return 0
}

func processTopperStage[K topkheap.Ordered, V proto.Message](
	partialData *common.TopperPartialData[K, V],
	data []V,
	taskIdentifier *protocol.TaskIdentifier,
	valueFunc func(input V) K,
) {
	taskID := model.TaskFragmentIdentifier{
		CreatorId:          taskIdentifier.GetCreatorId(),
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	if partialData.IsReady {
		log.Debugf("Partial data is ready, skipping task %s", taskID)
		return
	}

	if _, processed := partialData.TaskFragments[taskID]; processed {
		return
	}

	// Mark task as processed
	partialData.TaskFragments[taskID] = common.FragmentStatus{
		Logged: false,
	}

	// Aggregate data
	for _, item := range data {
		partialData.Heap.Insert(valueFunc(item), item)
	}

}

// Actualizar funciones para usar las constantes de etapas del paquete common
func (t *Topper) updateOmegaProcessed(clientId string, stage string) {
	switch stage {
	case common.EPSILON_STAGE:
		t.partialResults[clientId].EpsilonData.OmegaProcessed = true
	case common.THETA_STAGE:
		t.partialResults[clientId].ThetaData.MinPartialData.OmegaProcessed = true
		t.partialResults[clientId].ThetaData.MaxPartialData.OmegaProcessed = true
	case common.LAMBDA_STAGE:
		t.partialResults[clientId].LamdaData.OmegaProcessed = true
	}
}

func (t *Topper) updateRingRound(clientId string, stage string, round uint32) {
	switch stage {
	case common.EPSILON_STAGE:
		t.partialResults[clientId].EpsilonData.RingRound = round
	case common.THETA_STAGE:
		t.partialResults[clientId].ThetaData.MinPartialData.RingRound = round
		t.partialResults[clientId].ThetaData.MaxPartialData.RingRound = round
	case common.LAMBDA_STAGE:
		t.partialResults[clientId].LamdaData.RingRound = round
	}
}

func (m *Topper) LoadData(dirBase string) error {
	var err error
	m.partialResults, err = storage.LoadTopperPartialResultsFromDisk(dirBase)
	if err != nil {
		log.Errorf("Failed to load partial results from disk: %s", err)
		return err
	}

	return nil
}

func (t *Topper) SaveData(task *protocol.Task) error {
	stage := task.GetStage()
	clientId := task.GetClientId()
	taskIdentifier := task.GetTaskIdentifier()

	taskID := model.TaskFragmentIdentifier{
		CreatorId:          taskIdentifier.GetCreatorId(),
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	switch v := stage.(type) {
	case *protocol.Task_Epsilon:
		keyFunc := func(input *protocol.Epsilon_Data) uint64 {
			return input.GetTotalInvestment()
		}
		err := storage.SaveTopperDataToFile(t.infraConfig.GetDirectory(), stage, clientId, t.partialResults[clientId].EpsilonData, taskID, keyFunc)
		if err != nil {
			return fmt.Errorf("failed to save Epsilon data: %w", err)
		}

		t.partialResults[clientId].EpsilonData.TaskFragments[taskID] = common.FragmentStatus{
			Logged: true,
		}
		return nil

	case *protocol.Task_Lambda:
		keyFunc := func(input *protocol.Lambda_Data) uint64 {
			return input.GetParticipations()
		}
		err := storage.SaveTopperDataToFile(t.infraConfig.GetDirectory(), stage, clientId, t.partialResults[clientId].LamdaData, taskID, keyFunc)
		if err != nil {
			return fmt.Errorf("failed to save Lambda data: %w", err)
		}
		t.partialResults[clientId].LamdaData.TaskFragments[taskID] = common.FragmentStatus{
			Logged: true,
		}
		return nil

	case *protocol.Task_Theta:
		keyFunc := func(input *protocol.Theta_Data) float32 {
			return input.GetAvgRating()
		}

		partials := []*common.TopperPartialData[float32, *protocol.Theta_Data]{
			t.partialResults[clientId].ThetaData.MaxPartialData,
			t.partialResults[clientId].ThetaData.MinPartialData,
		}

		err := storage.SaveTopperThetaDataToFile(t.infraConfig.GetDirectory(), stage, clientId, partials, taskID, keyFunc)
		if err != nil {
			return fmt.Errorf("failed to save Theta data: %w", err)
		}
		t.partialResults[clientId].ThetaData.MaxPartialData.TaskFragments[taskID] = common.FragmentStatus{
			Logged: true,
		}
		t.partialResults[clientId].ThetaData.MinPartialData.TaskFragments[taskID] = common.FragmentStatus{
			Logged: true,
		}
		return nil

	case *protocol.Task_OmegaEOF:
		processedStage := task.GetOmegaEOF().GetData().GetStage()

		return t.saveMetadata(processedStage, clientId)

	case *protocol.Task_RingEOF:
		processedStage := task.GetRingEOF().GetStage()
		return t.saveMetadata(processedStage, clientId)

	default:
		return fmt.Errorf("invalid query stage: %v", v)
	}

}

func (t *Topper) saveMetadata(stage string, clientId string) error {
	switch stage {
	case common.EPSILON_STAGE:
		partialData := t.partialResults[clientId].EpsilonData
		return storage.SaveTopperMetadataToFile(
			t.infraConfig.GetDirectory(),
			clientId,
			stage,
			partialData,
		)
	case common.LAMBDA_STAGE:
		partialData := t.partialResults[clientId].LamdaData
		return storage.SaveTopperMetadataToFile(
			t.infraConfig.GetDirectory(),
			clientId,
			stage,
			partialData,
		)
	case common.THETA_STAGE:
		thetaData := t.partialResults[clientId].ThetaData.MaxPartialData

		return storage.SaveTopperMetadataToFile(
			t.infraConfig.GetDirectory(),
			clientId,
			stage,
			thetaData,
		)
	default:
		return fmt.Errorf("invalid stage for OmegaEOF: %s", stage)
	}
}

func (t *Topper) DeleteData(task *protocol.Task) error {
	stage := task.GetStage()
	clientId := task.GetClientId()

	switch stage.(type) {
	case *protocol.Task_RingEOF:
		processedStage := task.GetRingEOF().GetStage()
		return t.deleteStage(processedStage, clientId, string(common.GENERAL_FOLDER_TYPE))

	default:
		return nil
	}
}

func (t *Topper) deleteStage(stage string, clientId string, tableType string) error {
	switch stage {
	case common.EPSILON_STAGE:
		toDelete := t.partialResults[clientId].EpsilonData.IsReady
		return storage.TryDeletePartialData(t.infraConfig.GetDirectory(), stage, tableType, clientId, toDelete)

	case common.LAMBDA_STAGE:
		toDelete := t.partialResults[clientId].LamdaData.IsReady
		return storage.TryDeletePartialData(t.infraConfig.GetDirectory(), stage, tableType, clientId, toDelete)

	case common.THETA_STAGE:
		toDelete := t.partialResults[clientId].ThetaData.MaxPartialData.IsReady || t.partialResults[clientId].ThetaData.MinPartialData.IsReady
		return storage.TryDeletePartialData(t.infraConfig.GetDirectory(), stage, tableType, clientId, toDelete)

	default:
		return fmt.Errorf("invalid stage for deletion: %v", stage)

	}
}

func (t *Topper) setToDelete(clientId string, stage string) error {
	if _, ok := t.partialResults[clientId]; !ok {
		return fmt.Errorf("client %s not found", clientId)
	}
	switch stage {
	case common.LAMBDA_STAGE:
		t.partialResults[clientId].LamdaData.IsReady = true
	case common.EPSILON_STAGE:
		t.partialResults[clientId].EpsilonData.IsReady = true
	case common.THETA_STAGE:
		t.partialResults[clientId].ThetaData.MaxPartialData.IsReady = true
		t.partialResults[clientId].ThetaData.MinPartialData.IsReady = true

	default:
		return fmt.Errorf("invalid stage: %s", stage)
	}
	return nil
}
