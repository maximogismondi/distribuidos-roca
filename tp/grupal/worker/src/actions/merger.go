package actions

import (
	"fmt"
	"strconv"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils/storage"
)

const MERGER_STAGES_COUNT uint = 4

// Merger is a struct that implements the Action interface.
type Merger struct {
	infraConfig    *model.InfraConfig
	partialResults map[string]*common.MergerPartialResults
	itemHashFunc   func(workersCount int, item string) string
	eofHandler     *eof.StatefulEofHandler
}

func (m *Merger) makePartialResults(clientId string) {
	if _, ok := m.partialResults[clientId]; ok {
		return
	}

	m.partialResults[clientId] = &common.MergerPartialResults{
		Delta3: NewPartialData[*protocol.Delta_3_Data](),
		Eta3:   NewPartialData[*protocol.Eta_3_Data](),
		Kappa3: NewPartialData[*protocol.Kappa_3_Data](),
		Nu3:    NewPartialData[*protocol.Nu_3_Data](),
	}
}

// NewMerger creates a new Merger instance.
// It initializes the worker count and returns a pointer to the Merger struct.
func NewMerger(infraConfig *model.InfraConfig) *Merger {
	eofHandler := eof.NewStatefulEofHandler(
		model.MergerAction,
		infraConfig,
		mergerNextStageData,
		utils.GetWorkerIdFromHash,
	)

	merger := &Merger{
		infraConfig:    infraConfig,
		partialResults: make(map[string]*common.MergerPartialResults),
		itemHashFunc:   utils.GetWorkerIdFromHash,
		eofHandler:     eofHandler,
	}
	go storage.StartCleanupRoutine(infraConfig.GetDirectory(), infraConfig.GetCleanUpTime())
	return merger
}

func (m *Merger) delta3Stage(data []*protocol.Delta_3_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) (tasks common.Tasks) {
	partialData := m.partialResults[clientId].Delta3

	aggregationFunc := func(existing *protocol.Delta_3_Data, input *protocol.Delta_3_Data) {
		existing.PartialBudget += input.GetPartialBudget()
	}

	identifierFunc := func(input *protocol.Delta_3_Data) string {
		return input.GetCountry()
	}

	ProcessStage(partialData, data, clientId, taskIdentifier, aggregationFunc, identifierFunc, m.infraConfig, common.DELTA_STAGE_3)
	return nil
}

func (m *Merger) eta3Stage(data []*protocol.Eta_3_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) (tasks common.Tasks) {
	partialData := m.partialResults[clientId].Eta3

	aggregationFunc := func(existing *protocol.Eta_3_Data, input *protocol.Eta_3_Data) {
		existing.Rating += input.GetRating()
		existing.Count += input.GetCount()
	}

	identifierFunc := func(input *protocol.Eta_3_Data) string {
		return input.GetMovieId()
	}

	ProcessStage(partialData, data, clientId, taskIdentifier, aggregationFunc, identifierFunc, m.infraConfig, common.ETA_STAGE_3)
	return nil
}

func (m *Merger) kappa3Stage(data []*protocol.Kappa_3_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) (tasks common.Tasks) {
	partialData := m.partialResults[clientId].Kappa3

	aggregationFunc := func(existing *protocol.Kappa_3_Data, input *protocol.Kappa_3_Data) {
		existing.PartialParticipations += input.GetPartialParticipations()
	}

	identifierFunc := func(input *protocol.Kappa_3_Data) string {
		return input.GetActorId()
	}

	ProcessStage(partialData, data, clientId, taskIdentifier, aggregationFunc, identifierFunc, m.infraConfig, common.KAPPA_STAGE_3)
	return nil
}

func (m *Merger) nu3Stage(data []*protocol.Nu_3_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) (tasks common.Tasks) {
	partialData := m.partialResults[clientId].Nu3

	aggregationFunc := func(existing *protocol.Nu_3_Data, input *protocol.Nu_3_Data) {
		existing.Ratio += input.GetRatio()
		existing.Count += input.GetCount()
	}

	identifierFunc := func(input *protocol.Nu_3_Data) string {
		return strconv.FormatBool(input.GetSentiment())
	}

	ProcessStage(partialData, data, clientId, taskIdentifier, aggregationFunc, identifierFunc, m.infraConfig, common.NU_STAGE_3)
	return nil
}

func mergerNextStageData(stage string, clientId string, infraConfig *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
	switch stage {
	case common.DELTA_STAGE_3:
		return []common.NextStageData{
			{
				Stage:       common.EPSILON_STAGE,
				Exchange:    infraConfig.GetTopExchange(),
				WorkerCount: infraConfig.GetTopCount(),
				RoutingKey:  itemHashFunc(infraConfig.GetTopCount(), clientId+common.EPSILON_STAGE),
			},
		}, nil
	case common.ETA_STAGE_3:
		return []common.NextStageData{
			{
				Stage:       common.THETA_STAGE,
				Exchange:    infraConfig.GetTopExchange(),
				WorkerCount: infraConfig.GetTopCount(),
				RoutingKey:  itemHashFunc(infraConfig.GetTopCount(), clientId+common.THETA_STAGE),
			},
		}, nil
	case common.KAPPA_STAGE_3:
		return []common.NextStageData{
			{
				Stage:       common.LAMBDA_STAGE,
				Exchange:    infraConfig.GetTopExchange(),
				WorkerCount: infraConfig.GetTopCount(),
				RoutingKey:  itemHashFunc(infraConfig.GetTopCount(), clientId+common.LAMBDA_STAGE),
			},
		}, nil
	case common.NU_STAGE_3:
		return []common.NextStageData{
			{
				Stage:       model.RESULT_5_STAGE,
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
				WorkerCount: infraConfig.GetReduceCount(),
				RoutingKey:  utils.GetNextNodeId(infraConfig.GetNodeId(), infraConfig.GetMergeCount()),
			},
		}, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return []common.NextStageData{}, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (m *Merger) getNextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	return mergerNextStageData(stage, clientId, m.infraConfig, m.itemHashFunc)
}

func (m *Merger) delta3Results(tasks common.Tasks, clientId string) {
	partialDataMap := m.partialResults[clientId].Delta3.Data
	partialData := utils.MapValues(partialDataMap)
	results := utils.MapSlice(partialData, func(_ int, data *protocol.Delta_3_Data) *protocol.Epsilon_Data {
		return &protocol.Epsilon_Data{
			ProdCountry:     data.GetCountry(),
			TotalInvestment: data.GetPartialBudget(),
		}
	})

	identifierFunc := func(data *protocol.Epsilon_Data) string {
		return data.GetProdCountry()
	}

	taskDataCreator := func(stage string, data []*protocol.Epsilon_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Epsilon{
				Epsilon: &protocol.Epsilon{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := m.getNextStageData(common.DELTA_STAGE_3, clientId)
	hashFunc := func(workersCount int, _ string) string {
		return m.itemHashFunc(workersCount, clientId+common.EPSILON_STAGE)
	}

	creatorId := m.infraConfig.GetNodeId()
	taskNumber, _ := strconv.Atoi(creatorId)

	AddResultsToStateful(tasks, results, nextStageData[0], clientId, creatorId, taskNumber, hashFunc, identifierFunc, taskDataCreator, false)
}

func (m *Merger) eta3Results(tasks common.Tasks, clientId string) {
	partialDataMap := m.partialResults[clientId].Eta3.Data
	partialData := utils.MapValues(partialDataMap)

	results := utils.MapSlice(partialData, func(_ int, data *protocol.Eta_3_Data) *protocol.Theta_Data {
		return &protocol.Theta_Data{
			Id:        data.GetMovieId(),
			Title:     data.GetTitle(),
			AvgRating: float32(data.GetRating()) / float32(data.GetCount()),
		}
	})

	identifierFunc := func(data *protocol.Theta_Data) string {
		return data.GetId()
	}

	taskDataCreator := func(stage string, data []*protocol.Theta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Theta{
				Theta: &protocol.Theta{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := m.getNextStageData(common.ETA_STAGE_3, clientId)
	hashFunc := func(workersCount int, _ string) string {
		return m.itemHashFunc(workersCount, clientId+common.THETA_STAGE)
	}

	creatorId := m.infraConfig.GetNodeId()
	taskNumber, _ := strconv.Atoi(creatorId)

	AddResultsToStateful(tasks, results, nextStageData[0], clientId, creatorId, taskNumber, hashFunc, identifierFunc, taskDataCreator, false)
}

func (m *Merger) kappa3Results(tasks common.Tasks, clientId string) {
	partialDataMap := m.partialResults[clientId].Kappa3.Data
	partialData := utils.MapValues(partialDataMap)

	results := utils.MapSlice(partialData, func(_ int, data *protocol.Kappa_3_Data) *protocol.Lambda_Data {
		return &protocol.Lambda_Data{
			ActorId:        data.GetActorId(),
			ActorName:      data.GetActorName(),
			Participations: data.GetPartialParticipations(),
		}
	})

	identifierFunc := func(data *protocol.Lambda_Data) string {
		return data.GetActorId()
	}

	taskDataCreator := func(stage string, data []*protocol.Lambda_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Lambda{
				Lambda: &protocol.Lambda{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := m.getNextStageData(common.KAPPA_STAGE_3, clientId)
	hashFunc := func(workersCount int, _ string) string {
		return m.itemHashFunc(workersCount, clientId+common.LAMBDA_STAGE)
	}

	creatorId := m.infraConfig.GetNodeId()
	taskNumber, _ := strconv.Atoi(creatorId)

	AddResultsToStateful(tasks, results, nextStageData[0], clientId, creatorId, taskNumber, hashFunc, identifierFunc, taskDataCreator, false)
}

func (m *Merger) nu3Results(tasks common.Tasks, clientId string) {
	partialDataMap := m.partialResults[clientId].Nu3.Data
	partialData := utils.MapValues(partialDataMap)

	results := utils.MapSlice(partialData, func(_ int, data *protocol.Nu_3_Data) *protocol.Result5_Data {
		return &protocol.Result5_Data{
			Sentiment: data.GetSentiment(),
			Ratio:     data.GetRatio() / float32(data.GetCount()),
		}
	})

	taskDataCreator := func(stage string, data []*protocol.Result5_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Result5{
				Result5: &protocol.Result5{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := m.getNextStageData(common.NU_STAGE_3, clientId)

	creatorId := m.infraConfig.GetNodeId()
	taskNumber, _ := strconv.Atoi(creatorId)

	AddResultsToStateless(tasks, results, nextStageData[0], clientId, creatorId, taskNumber, taskDataCreator)
}

// Adding EOF handler to Merger
func (m *Merger) omegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) common.Tasks {
	tasks := make(common.Tasks)

	omegaReady, err := m.getOmegaProcessed(clientId, data.GetStage())
	if err != nil {
		log.Errorf("Failed to get omega ready for stage %s: %s", data.GetStage(), err)
		return tasks
	}
	if omegaReady {
		log.Debugf("Omega EOF for stage %s has already been processed for client %s", data.GetStage(), clientId)
		return tasks
	}

	m.updateOmegaProcessed(clientId, data.GetStage())

	m.eofHandler.HandleOmegaEOF(tasks, data, clientId)

	return tasks
}

func (m *Merger) ringEOFStage(data *protocol.RingEOF, clientId string) common.Tasks {
	tasks := make(common.Tasks)
	stage := data.GetStage()

	taskIdentifiers, err := m.getTaskIdentifiers(clientId, stage)
	if err != nil {
		log.Errorf("Failed to get task identifiers for stage %s: %s", stage, err)
		return tasks
	}

	ringRound, err := m.getRingRound(clientId, stage)
	if err != nil {
		log.Errorf("Failed to get ring round for stage %s: %s", stage, err)
		return tasks
	}
	if ringRound >= data.GetRoundNumber() {
		log.Debugf("Ring EOF for stage %s and client %s has already been processed for round %d", stage, clientId, ringRound)
		return tasks
	}

	m.updateRingRound(clientId, stage, data.GetRoundNumber())

	taskCount := m.participatesInResults(clientId, stage)
	ready := m.eofHandler.HandleRingEOF(tasks, data, clientId, taskIdentifiers, taskCount)

	if ready && taskCount > 0 {
		log.Warningf("ENTRE A READY EN STAGE %s for client %s", stage, clientId)
		err = m.addResultsToNextStage(tasks, stage, clientId)
		if err != nil {
			log.Errorf("Failed to add results to next stage for stage %s: %s", stage, err)
			return tasks
		}
		err := m.setToDelete(clientId, data.GetStage())
		if err != nil {
			log.Errorf("Failed to set delete for stage %s for client %s: %s", data.GetStage(), clientId, err)
		}
	}

	return tasks
}

// Adjusting Execute to pass taskIdentifier
func (m *Merger) Execute(task *protocol.Task) (common.Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()
	taskIdentifier := task.GetTaskIdentifier()

	m.makePartialResults(clientId)

	switch v := stage.(type) {
	case *protocol.Task_Delta_3:
		data := v.Delta_3.GetData()
		return m.delta3Stage(data, clientId, taskIdentifier), nil

	case *protocol.Task_Eta_3:
		data := v.Eta_3.GetData()
		return m.eta3Stage(data, clientId, taskIdentifier), nil

	case *protocol.Task_Kappa_3:
		data := v.Kappa_3.GetData()
		return m.kappa3Stage(data, clientId, taskIdentifier), nil

	case *protocol.Task_Nu_3:
		data := v.Nu_3.GetData()
		return m.nu3Stage(data, clientId, taskIdentifier), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return m.omegaEOFStage(data, clientId), nil

	case *protocol.Task_RingEOF:
		return m.ringEOFStage(v.RingEOF, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}

func (m *Merger) LoadData(dirBase string) error {
	var err error
	m.partialResults, err = storage.LoadMergerPartialResultsFromDisk(dirBase)
	if err != nil {
		log.Errorf("Failed to load partial results from disk: %s", err)
		return err
	}

	return nil
}

func (m *Merger) SaveData(task *protocol.Task) error {

	stage := task.GetStage()
	clientId := task.GetClientId()
	taskIdentifier := task.GetTaskIdentifier()

	taskID := model.TaskFragmentIdentifier{
		CreatorId:          taskIdentifier.GetCreatorId(),
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	switch s := stage.(type) {
	case *protocol.Task_Delta_3:
		err := storage.SaveDataToFile(m.infraConfig.GetDirectory(), clientId, s, common.GENERAL_FOLDER_TYPE, m.partialResults[clientId].Delta3, taskID)
		if err != nil {
			return fmt.Errorf("failed to save Delta 3 data: %w", err)
		}

		m.partialResults[clientId].Delta3.TaskFragments[taskID] = common.FragmentStatus{
			Logged: true,
		}
		return nil

	case *protocol.Task_Eta_3:
		err := storage.SaveDataToFile(m.infraConfig.GetDirectory(), clientId, s, common.GENERAL_FOLDER_TYPE, m.partialResults[clientId].Eta3, taskID)
		if err != nil {
			return fmt.Errorf("failed to save Eta 3 data: %w", err)
		}

		m.partialResults[clientId].Eta3.TaskFragments[taskID] = common.FragmentStatus{
			Logged: true,
		}
		return nil

	case *protocol.Task_Kappa_3:
		err := storage.SaveDataToFile(m.infraConfig.GetDirectory(), clientId, s, common.GENERAL_FOLDER_TYPE, m.partialResults[clientId].Kappa3, taskID)
		if err != nil {
			return fmt.Errorf("failed to save Kappa 3 data: %w", err)
		}

		m.partialResults[clientId].Kappa3.TaskFragments[taskID] = common.FragmentStatus{
			Logged: true,
		}
		return nil
	case *protocol.Task_Nu_3:
		err := storage.SaveDataToFile(m.infraConfig.GetDirectory(), clientId, s, common.GENERAL_FOLDER_TYPE, m.partialResults[clientId].Nu3, taskID)
		if err != nil {
			return fmt.Errorf("failed to save Nu 3 data: %w", err)
		}

		m.partialResults[clientId].Nu3.TaskFragments[taskID] = common.FragmentStatus{
			Logged: true,
		}
		return nil

	case *protocol.Task_OmegaEOF:
		processedStage := task.GetOmegaEOF().GetData().GetStage()
		log.Debugf("SAVE OMEGA EOF FOR STAGE %s AND CLIENT %s", processedStage, clientId)
		return m.saveMetaData(processedStage, clientId)

	case *protocol.Task_RingEOF:
		log.Debugf("SAVE RING EOF FOR STAGE %s AND CLIENT %s", task.GetRingEOF().GetStage(), clientId)
		processedStage := task.GetRingEOF().GetStage()
		return m.saveMetaData(processedStage, clientId)

	default:
		return fmt.Errorf("invalid query stage: %v", s)
	}

}

func (m *Merger) saveMetaData(stage string, clientId string) error {
	switch stage {
	case common.DELTA_STAGE_3:
		partialData := m.partialResults[clientId].Delta3
		log.Debugf("SAVE DELTA 3 FOR STAGE %s AND READY IS %v", stage, partialData.IsReady)
		log.Debugf("OMEGA PROCESSED IS %v", partialData.OmegaProcessed)
		return storage.SaveMetadataToFile(
			m.infraConfig.GetDirectory(),
			clientId,
			stage,
			common.GENERAL_FOLDER_TYPE,
			partialData,
		)
	case common.ETA_STAGE_3:
		partialData := m.partialResults[clientId].Eta3
		log.Debugf("SAVE ETA 3 FOR STAGE %s AND READY IS %v", stage, partialData.IsReady)
		log.Debugf("OMEGA PROCESSED IS %v", partialData.OmegaProcessed)
		return storage.SaveMetadataToFile(
			m.infraConfig.GetDirectory(),
			clientId,
			stage,
			common.GENERAL_FOLDER_TYPE,
			partialData,
		)
	case common.KAPPA_STAGE_3:
		partialData := m.partialResults[clientId].Kappa3
		log.Debugf("SAVE KAPPA 3 FOR STAGE %s AND READY IS %v", stage, partialData.IsReady)
		log.Debugf("OMEGA PROCESSED IS %v", partialData.OmegaProcessed)
		return storage.SaveMetadataToFile(
			m.infraConfig.GetDirectory(),
			clientId,
			stage,
			common.GENERAL_FOLDER_TYPE,
			partialData,
		)
	case common.NU_STAGE_3:
		partialData := m.partialResults[clientId].Nu3
		log.Debugf("SAVE NU 3 FOR STAGE %s AND READY IS %v", stage, partialData.IsReady)
		log.Debugf("OMEGA PROCESSED IS %v", partialData.OmegaProcessed)
		return storage.SaveMetadataToFile(
			m.infraConfig.GetDirectory(),
			clientId,
			stage,
			common.GENERAL_FOLDER_TYPE,
			partialData,
		)
	default:
		return fmt.Errorf("invalid stage for OmegaEOF: %s", stage)
	}
}

func (m *Merger) DeleteData(task *protocol.Task) error {
	stage := task.GetStage()
	clientId := task.GetClientId()
	tableType := task.GetTableType()

	switch stage.(type) {
	case *protocol.Task_RingEOF:
		processedStage := task.GetRingEOF().GetStage()
		return m.deleteStage(processedStage, clientId, tableType)

	default:
		return nil
	}
}

func (m *Merger) deleteStage(stage string, clientId string, tableType string) error {

	switch stage {
	case common.DELTA_STAGE_3:
		toDelete := m.partialResults[clientId].Delta3.IsReady
		return storage.TryDeletePartialData(m.infraConfig.GetDirectory(), common.DELTA_STAGE_3, tableType, clientId, toDelete)

	case common.ETA_STAGE_3:
		toDelete := m.partialResults[clientId].Eta3.IsReady
		return storage.TryDeletePartialData(m.infraConfig.GetDirectory(), common.ETA_STAGE_3, tableType, clientId, toDelete)

	case common.KAPPA_STAGE_3:
		toDelete := m.partialResults[clientId].Kappa3.IsReady
		return storage.TryDeletePartialData(m.infraConfig.GetDirectory(), common.KAPPA_STAGE_3, tableType, clientId, toDelete)

	case common.NU_STAGE_3:
		toDelete := m.partialResults[clientId].Nu3.IsReady
		return storage.TryDeletePartialData(m.infraConfig.GetDirectory(), common.NU_STAGE_3, tableType, clientId, toDelete)

	default:
		return fmt.Errorf("invalid stage for deletion: %v", stage)

	}
}

func (m *Merger) setToDelete(clientId string, stage string) error {
	if _, ok := m.partialResults[clientId]; !ok {
		return fmt.Errorf("client %s not found", clientId)
	}

	log.Debugf("SETTING TO DELETE IN MERGER FOR STAGE %s AND CLIENT %s", stage, clientId)
	switch stage {
	case common.DELTA_STAGE_3:
		m.partialResults[clientId].Delta3.IsReady = true
	case common.ETA_STAGE_3:
		m.partialResults[clientId].Eta3.IsReady = true
	case common.KAPPA_STAGE_3:
		m.partialResults[clientId].Kappa3.IsReady = true
	case common.NU_STAGE_3:
		m.partialResults[clientId].Nu3.IsReady = true
	default:
		return fmt.Errorf("invalid stage: %s", stage)
	}
	return nil
}

// Implementing getTaskIdentifiers for Merger
func (m *Merger) getTaskIdentifiers(clientId string, stage string) ([]model.TaskFragmentIdentifier, error) {
	partialResults := m.partialResults[clientId]
	switch stage {
	case common.DELTA_STAGE_3:
		return utils.MapKeys(partialResults.Delta3.TaskFragments), nil
	case common.ETA_STAGE_3:
		return utils.MapKeys(partialResults.Eta3.TaskFragments), nil
	case common.KAPPA_STAGE_3:
		return utils.MapKeys(partialResults.Kappa3.TaskFragments), nil
	case common.NU_STAGE_3:
		return utils.MapKeys(partialResults.Nu3.TaskFragments), nil
	default:
		return nil, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (m *Merger) participatesInResults(clientId string, stage string) int {
	partialResults, ok := m.partialResults[clientId]
	if !ok {
		return 0
	}

	participates := false

	switch stage {
	case common.DELTA_STAGE_3:
		participates = len(partialResults.Delta3.Data) > 0
	case common.ETA_STAGE_3:
		participates = len(partialResults.Eta3.Data) > 0
	case common.KAPPA_STAGE_3:
		participates = len(partialResults.Kappa3.Data) > 0
	case common.NU_STAGE_3:
		participates = len(partialResults.Nu3.Data) > 0
	default:
		log.Errorf("Invalid stage: %s", stage)
		return 0
	}

	if participates {
		return 1
	}
	return 0
}

func (m *Merger) addResultsToNextStage(tasks common.Tasks, stage string, clientId string) error {
	switch stage {
	case common.DELTA_STAGE_3:
		m.delta3Results(tasks, clientId)
	case common.ETA_STAGE_3:
		m.eta3Results(tasks, clientId)
	case common.KAPPA_STAGE_3:
		m.kappa3Results(tasks, clientId)
	case common.NU_STAGE_3:
		m.nu3Results(tasks, clientId)
	default:
		return fmt.Errorf("invalid stage: %s", stage)
	}

	return nil
}

func (m *Merger) getOmegaProcessed(clientId string, stage string) (bool, error) {
	partialResults := m.partialResults[clientId]
	switch stage {
	case common.DELTA_STAGE_3:
		return partialResults.Delta3.OmegaProcessed, nil
	case common.ETA_STAGE_3:
		return partialResults.Eta3.OmegaProcessed, nil
	case common.KAPPA_STAGE_3:
		return partialResults.Kappa3.OmegaProcessed, nil
	case common.NU_STAGE_3:
		return partialResults.Nu3.OmegaProcessed, nil
	default:
		return false, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (m *Merger) getRingRound(clientId string, stage string) (uint32, error) {
	partialResults := m.partialResults[clientId]
	switch stage {
	case common.DELTA_STAGE_3:
		return partialResults.Delta3.RingRound, nil
	case common.ETA_STAGE_3:
		return partialResults.Eta3.RingRound, nil
	case common.KAPPA_STAGE_3:
		return partialResults.Kappa3.RingRound, nil
	case common.NU_STAGE_3:
		return partialResults.Nu3.RingRound, nil
	default:
		return 0, fmt.Errorf("invalid stage: %s", stage)
	}
}

// Actualizar funciones para usar las constantes de etapas del paquete common
func (m *Merger) updateOmegaProcessed(clientId string, stage string) {
	log.Debugf("UPDATING OMEGA PROCESSED IN MERGER FOR STAGE %s AND CLIENT %s", stage, clientId)
	switch stage {
	case common.DELTA_STAGE_3:
		m.partialResults[clientId].Delta3.OmegaProcessed = true
	case common.ETA_STAGE_3:
		m.partialResults[clientId].Eta3.OmegaProcessed = true
	case common.KAPPA_STAGE_3:
		m.partialResults[clientId].Kappa3.OmegaProcessed = true
	case common.NU_STAGE_3:
		m.partialResults[clientId].Nu3.OmegaProcessed = true
	}
}

func (m *Merger) updateRingRound(clientId string, stage string, round uint32) {
	log.Debugf("UPDATING RING ROUND IN MERGER FOR STAGE %s AND CLIENT %s", stage, clientId)
	switch stage {
	case common.DELTA_STAGE_3:
		m.partialResults[clientId].Delta3.RingRound = round
	case common.ETA_STAGE_3:
		m.partialResults[clientId].Eta3.RingRound = round
	case common.KAPPA_STAGE_3:
		m.partialResults[clientId].Kappa3.RingRound = round
	case common.NU_STAGE_3:
		m.partialResults[clientId].Nu3.RingRound = round
	}
}
