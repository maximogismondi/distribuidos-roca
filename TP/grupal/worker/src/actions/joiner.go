package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils/storage"
	"google.golang.org/protobuf/proto"
)

func newJoinerTableData[T any](data T) *common.JoinerTableData[T] {
	return &common.JoinerTableData[T]{
		Data:          data,
		TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
		Ready:         false,
	}
}

func newJoinerStageData[S, B proto.Message]() *common.JoinerStageData[S, B] {
	return &common.JoinerStageData[S, B]{
		SmallTable:          newJoinerTableData(make(common.SmallTableData[S])),
		BigTable:            newJoinerTableData(make(common.BigTableData[B])),
		SendedTaskCount:     0,
		SmallTableTaskCount: 0,
		RingRound:           0,
	}
}

func newJoinerPartialResults() *common.JoinerPartialResults {
	return &common.JoinerPartialResults{
		ZetaData: newJoinerStageData[*protocol.Zeta_Data_Movie, *protocol.Zeta_Data_Rating](),
		IotaData: newJoinerStageData[*protocol.Iota_Data_Movie, *protocol.Iota_Data_Actor](),
	}
}

// Joiner is a struct that implements the Action interface.
type Joiner struct {
	infraConfig    *model.InfraConfig
	partialResults map[string]*common.JoinerPartialResults
	itemHashFunc   func(workersCount int, item string) string
	randomHashFunc func(workersCount int) string
	eofHandler     *eof.StatefulEofHandler
}

func (j *Joiner) makePartialResults(clientId string) {
	if _, ok := j.partialResults[clientId]; ok {
		log.Debugf("[Joiner] makePartialResults: partialResults[%s] ya existe", clientId)
		return
	}
	log.Debugf("[Joiner] makePartialResults: inicializando partialResults[%s]", clientId)
	j.partialResults[clientId] = newJoinerPartialResults()
	log.Debugf("[Joiner] makePartialResults: después de inicializar, partialResults[%s]=%#v", clientId, j.partialResults[clientId])
}

// NewJoiner creates a new Joiner instance.
func NewJoiner(infraConfig *model.InfraConfig) *Joiner {
	eofHandler := eof.NewStatefulEofHandler(
		model.JoinerAction,
		infraConfig,
		joinerNextStageData,
		utils.GetWorkerIdFromHash,
	)

	joiner := &Joiner{
		infraConfig:    infraConfig,
		itemHashFunc:   utils.GetWorkerIdFromHash,
		randomHashFunc: utils.RandomHash,
		partialResults: make(map[string]*common.JoinerPartialResults),
		eofHandler:     eofHandler,
	}

	log.Infof("[Joiner] NewJoiner: inicializado con infraConfig=%#v", infraConfig)
	go storage.StartCleanupRoutine(infraConfig.GetDirectory(), infraConfig.GetCleanUpTime())

	return joiner
}

func (j *Joiner) joinZetaData(ratingsData common.BigTableData[*protocol.Zeta_Data_Rating], clientId string) map[int][]*protocol.Eta_1_Data {
	joinedDataByTask := make(map[int][]*protocol.Eta_1_Data)
	for taskNumber, ratingsByTask := range ratingsData {
		joinedData := make([]*protocol.Eta_1_Data, 0)

		for movieId, ratings := range ratingsByTask {
			movieData, ok := j.partialResults[clientId].ZetaData.SmallTable.Data[movieId]
			if !ok {
				continue
			}

			for _, rating := range ratings {
				joinedData = append(joinedData, &protocol.Eta_1_Data{
					MovieId: movieId,
					Title:   movieData.GetTitle(),
					Rating:  rating.GetRating(),
				})
			}
		}

		if len(joinedData) > 0 {
			joinedDataByTask[taskNumber] = joinedData
		}
	}

	return joinedDataByTask
}

func (j *Joiner) joinIotaData(actorsData common.BigTableData[*protocol.Iota_Data_Actor], clientId string) map[int][]*protocol.Kappa_1_Data {
	joinedDataByTask := make(map[int][]*protocol.Kappa_1_Data)

	for taskNumber, actorsByTask := range actorsData {
		joinedData := make([]*protocol.Kappa_1_Data, 0)

		for movieId, actors := range actorsByTask {
			_, ok := j.partialResults[clientId].IotaData.SmallTable.Data[movieId]
			if !ok {
				continue
			}

			for _, actor := range actors {
				joinedData = append(joinedData, &protocol.Kappa_1_Data{
					MovieId:   movieId,
					ActorId:   actor.GetActorId(),
					ActorName: actor.GetActorName(),
				})
			}
		}

		if len(joinedData) > 0 {
			joinedDataByTask[taskNumber] = joinedData
		}
	}

	return joinedDataByTask
}

func (j *Joiner) moviesZetaStage(data []*protocol.Zeta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	tasks := make(common.Tasks)
	zetaData := j.partialResults[clientId].ZetaData

	identifierFunc := func(input *protocol.Zeta_Data) string {
		return input.GetMovie().GetMovieId()
	}

	mappingFunc := func(input *protocol.Zeta_Data) *protocol.Zeta_Data_Movie {
		return &protocol.Zeta_Data_Movie{
			MovieId: input.GetMovie().GetMovieId(),
			Title:   input.GetMovie().GetTitle(),
		}
	}

	smallTable := zetaData.SmallTable

	processSmallTableJoinerStage(smallTable, data, clientId, taskIdentifier, identifierFunc, mappingFunc)

	omegaProcessed := smallTable.OmegaProcessed

	smallTableReady := omegaProcessed && len(smallTable.TaskFragments) == zetaData.SmallTableTaskCount
	smallTable.Ready = smallTableReady

	if smallTableReady {
		bigTable := zetaData.BigTable

		partialResults := j.joinZetaData(bigTable.Data, clientId)
		j.addEta1Results(tasks, partialResults, clientId)

		bigTable.Data = make(common.BigTableData[*protocol.Zeta_Data_Rating])

		if bigTable.Ready {
			zetaData.SmallTable.IsReadyToDelete = true
			zetaData.BigTable.IsReadyToDelete = true
		} else {
			zetaData.BigTable.IsReadyToDelete = true
		}

	}

	return tasks
}

func (j *Joiner) moviesIotaStage(data []*protocol.Iota_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	tasks := make(common.Tasks)
	iotaData := j.partialResults[clientId].IotaData

	identifierFunc := func(input *protocol.Iota_Data) string {
		return input.GetMovie().GetMovieId()
	}

	mappingFunc := func(input *protocol.Iota_Data) *protocol.Iota_Data_Movie {
		return &protocol.Iota_Data_Movie{
			MovieId: input.GetMovie().GetMovieId(),
		}
	}

	smallTable := iotaData.SmallTable

	processSmallTableJoinerStage(smallTable, data, clientId, taskIdentifier, identifierFunc, mappingFunc)

	omegaProcessed := smallTable.OmegaProcessed

	smallTableReady := omegaProcessed && len(smallTable.TaskFragments) == iotaData.SmallTableTaskCount
	smallTable.Ready = smallTableReady

	if smallTableReady {
		bigTable := iotaData.BigTable

		partialResults := j.joinIotaData(bigTable.Data, clientId)
		j.addKappa1Results(tasks, partialResults, clientId)

		bigTable.Data = make(common.BigTableData[*protocol.Iota_Data_Actor])

		if bigTable.Ready {
			iotaData.SmallTable.IsReadyToDelete = true
			iotaData.BigTable.IsReadyToDelete = true
		} else {
			iotaData.BigTable.IsReadyToDelete = true
		}
	}

	return tasks
}

func (j *Joiner) ratingsZetaStage(data []*protocol.Zeta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	tasks := make(common.Tasks)

	zetaData := j.partialResults[clientId].ZetaData
	partialData := zetaData.BigTable

	identifierFunc := func(input *protocol.Zeta_Data) string {
		return input.GetRating().GetMovieId()
	}

	mappingFunc := func(input *protocol.Zeta_Data) *protocol.Zeta_Data_Rating {
		return &protocol.Zeta_Data_Rating{
			MovieId: input.GetRating().GetMovieId(),
			Rating:  input.GetRating().GetRating(),
		}
	}

	processBigTableJoinerStage(partialData, data, clientId, taskIdentifier, identifierFunc, mappingFunc)

	if zetaData.SmallTable.Ready {
		partialResults := j.joinZetaData(partialData.Data, clientId)
		j.addEta1Results(tasks, partialResults, clientId)

		partialData.Data = make(common.BigTableData[*protocol.Zeta_Data_Rating])
	}

	// TODO - not ready -- save data

	return tasks
}

func (j *Joiner) actorsIotaStage(data []*protocol.Iota_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	tasks := make(common.Tasks)

	iotaData := j.partialResults[clientId].IotaData
	partialData := iotaData.BigTable

	identifierFunc := func(input *protocol.Iota_Data) string {
		return input.GetActor().GetMovieId()
	}

	mappingFunc := func(input *protocol.Iota_Data) *protocol.Iota_Data_Actor {
		return &protocol.Iota_Data_Actor{
			MovieId:   input.GetActor().GetMovieId(),
			ActorId:   input.GetActor().GetActorId(),
			ActorName: input.GetActor().GetActorName(),
		}
	}

	processBigTableJoinerStage(partialData, data, clientId, taskIdentifier, identifierFunc, mappingFunc)

	if iotaData.SmallTable.Ready {
		partialResults := j.joinIotaData(partialData.Data, clientId)
		j.addKappa1Results(tasks, partialResults, clientId)

		partialData.Data = make(common.BigTableData[*protocol.Iota_Data_Actor])

	}

	// TODO - not ready -- save data

	return tasks
}

func (j *Joiner) zetaStage(data []*protocol.Zeta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier, tableType string) common.Tasks {
	switch tableType {
	case model.SMALL_TABLE:
		return j.moviesZetaStage(data, clientId, taskIdentifier)
	case model.BIG_TABLE:
		return j.ratingsZetaStage(data, clientId, taskIdentifier)
	default:
		log.Errorf("Invalid table type: %s", tableType)
		return nil
	}
}

func (j *Joiner) iotaStage(data []*protocol.Iota_Data, clientId string, taskIdentifier *protocol.TaskIdentifier, tableType string) common.Tasks {

	switch tableType {
	case model.SMALL_TABLE:
		return j.moviesIotaStage(data, clientId, taskIdentifier)
	case model.BIG_TABLE:
		return j.actorsIotaStage(data, clientId, taskIdentifier)
	default:
		log.Errorf("Invalid table type: %s", tableType)
		return nil
	}
}

func (j *Joiner) nextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	return joinerNextStageData(stage, clientId, j.infraConfig, j.itemHashFunc)
}

func joinerNextStageData(stage string, clientId string, infraConfig *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
	switch stage {
	case common.ZETA_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.ETA_STAGE_1,
				Exchange:    infraConfig.GetMapExchange(),
				WorkerCount: infraConfig.GetMapCount(),
				RoutingKey:  infraConfig.GetBroadcastID(),
			},
		}, nil
	case common.IOTA_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.KAPPA_STAGE_1,
				Exchange:    infraConfig.GetMapExchange(),
				WorkerCount: infraConfig.GetMapCount(),
				RoutingKey:  infraConfig.GetBroadcastID(),
			},
		}, nil
	case common.RING_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.RING_STAGE,
				Exchange:    infraConfig.GetEofExchange(),
				WorkerCount: infraConfig.GetJoinCount(),
				RoutingKey:  utils.GetNextNodeId(infraConfig.GetNodeId(), infraConfig.GetJoinCount()),
			},
		}, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return []common.NextStageData{}, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (j *Joiner) smallTableOmegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks common.Tasks) {
	tasks = make(common.Tasks)

	// Estas 2 los dejo por si se necesita para el borrado
	var ready bool
	// var bigTableReady bool

	taskCount := int(data.GetTasksCount())

	switch data.GetStage() {
	case common.ZETA_STAGE:
		zetaData := j.partialResults[clientId].ZetaData

		if zetaData.SmallTable.OmegaProcessed {
			log.Infof("OmegaEOF for small table already processed for clientId: %s", clientId)
			return tasks
		}

		zetaData.SmallTableTaskCount = taskCount
		zetaData.SmallTable.OmegaProcessed = true

		ready = len(zetaData.SmallTable.TaskFragments) == taskCount
		zetaData.SmallTable.Ready = ready

		if ready {
			partialResults := j.joinZetaData(zetaData.BigTable.Data, clientId)
			j.addEta1Results(tasks, partialResults, clientId)

			zetaData.BigTable.Data = make(common.BigTableData[*protocol.Zeta_Data_Rating])

			if zetaData.BigTable.Ready {
				zetaData.SmallTable.IsReadyToDelete = true
			}
			zetaData.BigTable.IsReadyToDelete = true
		}

		// bigTableReady = zetaData.bigTable.ready

	case common.IOTA_STAGE:
		iotaData := j.partialResults[clientId].IotaData

		if iotaData.SmallTable.OmegaProcessed {
			log.Infof("OmegaEOF for small table already processed for clientId: %s", clientId)
			return tasks
		}

		iotaData.SmallTableTaskCount = taskCount
		iotaData.SmallTable.OmegaProcessed = true

		ready = len(iotaData.SmallTable.TaskFragments) == taskCount
		iotaData.SmallTable.Ready = ready

		if ready {
			partialResults := j.joinIotaData(iotaData.BigTable.Data, clientId)
			j.addKappa1Results(tasks, partialResults, clientId)

			iotaData.BigTable.Data = make(common.BigTableData[*protocol.Iota_Data_Actor])

			if iotaData.BigTable.Ready {
				iotaData.SmallTable.IsReadyToDelete = true
				iotaData.BigTable.IsReadyToDelete = true

			} else {
				iotaData.BigTable.IsReadyToDelete = true
			}
		}

		// bigTableReady = iotaData.bigTable.ready
	}

	return tasks
}

func (j *Joiner) bigTableOmegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks common.Tasks) {
	tasks = make(common.Tasks)

	switch data.GetStage() {
	case common.ZETA_STAGE:
		zetaData := j.partialResults[clientId].ZetaData

		if zetaData.BigTable.OmegaProcessed {
			log.Infof("OmegaEOF for big table already processed for clientId: %s", clientId)
			return tasks
		}

		zetaData.BigTable.OmegaProcessed = true
		j.eofHandler.HandleOmegaEOF(tasks, data, clientId)
	case common.IOTA_STAGE:
		iotaData := j.partialResults[clientId].IotaData

		if iotaData.BigTable.OmegaProcessed {
			log.Infof("OmegaEOF for big table already processed for clientId: %s", clientId)
			return tasks
		}

		iotaData.BigTable.OmegaProcessed = true
		j.eofHandler.HandleOmegaEOF(tasks, data, clientId)
	}

	return tasks
}

func (j *Joiner) omegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks common.Tasks) {
	switch data.EofType {
	case model.SMALL_TABLE:
		return j.smallTableOmegaEOFStage(data, clientId)
	case model.BIG_TABLE:
		return j.bigTableOmegaEOFStage(data, clientId)
	default:
		return nil
	}
}

func (j *Joiner) ringEOFStage(data *protocol.RingEOF, clientId string) (tasks common.Tasks) {
	tasks = make(common.Tasks)

	var smallTableReady bool
	var bigTableReady bool

	switch data.GetStage() {
	case common.ZETA_STAGE:
		zetaData := j.partialResults[clientId].ZetaData
		ringRound := data.GetRoundNumber()

		if zetaData.RingRound >= ringRound {
			log.Infof("RingEOF for Zeta stage already processed for clientId: %s, round: %d", clientId, zetaData.RingRound)
			return tasks
		}

		zetaData.RingRound = ringRound

		resultsTaskCount := zetaData.SendedTaskCount
		taskFragments := []model.TaskFragmentIdentifier{}

		smallTableReady = zetaData.SmallTable.Ready
		if smallTableReady {
			taskFragments = utils.MapKeys(zetaData.BigTable.TaskFragments)
		}

		bigTableReady = j.eofHandler.HandleRingEOF(tasks, data, clientId, taskFragments, resultsTaskCount)
		zetaData.BigTable.Ready = bigTableReady

		if bigTableReady && smallTableReady {
			// log.Infof("RingEOF for Zeta stage is ready to delete for clientId: %s", clientId)
			zetaData.SmallTable.IsReadyToDelete = true
			zetaData.BigTable.IsReadyToDelete = true
		}

	case common.IOTA_STAGE:
		iotaData := j.partialResults[clientId].IotaData
		ringRound := data.GetRoundNumber()

		if iotaData.RingRound >= ringRound {
			log.Infof("RingEOF for Iota stage already processed for clientId: %s, round: %d", clientId, iotaData.RingRound)
			return tasks
		}

		iotaData.RingRound = ringRound

		resultsTaskCount := iotaData.SendedTaskCount
		taskFragments := []model.TaskFragmentIdentifier{}

		smallTableReady = iotaData.SmallTable.Ready
		if smallTableReady {
			taskFragments = utils.MapKeys(iotaData.BigTable.TaskFragments)
		}

		bigTableReady = j.eofHandler.HandleRingEOF(tasks, data, clientId, taskFragments, resultsTaskCount)
		iotaData.BigTable.Ready = bigTableReady

		if bigTableReady && smallTableReady {
			// log.Infof("RingEOF for Iota stage is ready to delete for clientId: %s", clientId)
			iotaData.SmallTable.IsReadyToDelete = true
			iotaData.BigTable.IsReadyToDelete = true
		}
	}

	return tasks
	// if ready {
	// 	if err := storage.DeletePartialResults(j.infraConfig.GetDirectory(), clientId, data.GetStage(), common.ANY_SOURCE); err != nil {
	// 		log.Errorf("Failed to delete partial results: %s", err)
	// 	}

	// 	j.DeleteStage(clientId, data.GetStage())

	// 	if len(j.partialResults[clientId].zetaData.smallTable.data) == 0 &&
	// 		len(j.partialResults[clientId].iotaData.smallTable.data) == 0 &&
	// 		len(j.partialResults[clientId].zetaData.bigTable.data) == 0 &&
	// 		len(j.partialResults[clientId].iotaData.bigTable.data) == 0 {
	// 		j.deletePartialResult(clientId)
	// 	}
	// }
}

func (j *Joiner) Execute(task *protocol.Task) (common.Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()
	taskIdentifier := task.GetTaskIdentifier()
	tableType := task.GetTableType()

	j.makePartialResults(clientId)

	switch v := stage.(type) {
	case *protocol.Task_Zeta:
		data := v.Zeta.GetData()
		return j.zetaStage(data, clientId, taskIdentifier, tableType), nil

	case *protocol.Task_Iota:
		data := v.Iota.GetData()
		return j.iotaStage(data, clientId, taskIdentifier, tableType), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return j.omegaEOFStage(data, clientId), nil

	case *protocol.Task_RingEOF:
		return j.ringEOFStage(v.RingEOF, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}

func (j *Joiner) addEta1Results(tasks common.Tasks, partialResultsByTask map[int][]*protocol.Eta_1_Data, clientId string) {
	dataStage := common.ZETA_STAGE
	zetaData := j.partialResults[clientId].ZetaData

	nextStageData, _ := j.nextStageData(dataStage, clientId)

	taskDataCreator := func(stage string, data []*protocol.Eta_1_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Eta_1{
				Eta_1: &protocol.Eta_1{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	creatorId := j.infraConfig.GetNodeId()

	for taskNumber, partialResults := range partialResultsByTask {
		AddResultsToStateless(tasks, partialResults, nextStageData[0], clientId, creatorId, taskNumber, taskDataCreator)
		zetaData.SendedTaskCount++
	}

}

func (j *Joiner) addKappa1Results(tasks common.Tasks, partialResultsByTask map[int][]*protocol.Kappa_1_Data, clientId string) {
	dataStage := common.IOTA_STAGE
	iotaData := j.partialResults[clientId].IotaData

	nextStageData, _ := j.nextStageData(dataStage, clientId)

	taskDataCreator := func(stage string, data []*protocol.Kappa_1_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Kappa_1{
				Kappa_1: &protocol.Kappa_1{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	creatorId := j.infraConfig.GetNodeId()

	for taskNumber, partialResults := range partialResultsByTask {
		AddResultsToStateless(tasks, partialResults, nextStageData[0], clientId, creatorId, taskNumber, taskDataCreator)
		iotaData.SendedTaskCount++
	}
}

func processSmallTableJoinerStage[S, G proto.Message](
	partialData *common.JoinerTableData[common.SmallTableData[S]],
	data []G,
	clientId string,
	taskIdentifier *protocol.TaskIdentifier,
	identifierFunc func(input G) string,
	mappingFunc func(input G) S,
) {
	taskID := model.TaskFragmentIdentifier{
		CreatorId:          taskIdentifier.GetCreatorId(),
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	if partialData.Ready {
		log.Infof("Task fragment %v is ready to delete for clientId: %s", taskID, clientId)
		return
	}

	if _, exists := partialData.TaskFragments[taskID]; exists {
		log.Infof("Task fragment %v already processed for clientId: %s", taskID, clientId)
		return
	}

	partialData.TaskFragments[taskID] = common.FragmentStatus{
		Logged: false,
	}

	for _, item := range data {
		id := identifierFunc(item)
		partialData.Data[id] = mappingFunc(item)
	}
}

func processBigTableJoinerStage[B, G proto.Message](
	partialData *common.JoinerTableData[common.BigTableData[B]],
	data []G,
	clientId string,
	taskIdentifier *protocol.TaskIdentifier,
	identifierFunc func(input G) string,
	mappingFunc func(input G) B,
) {
	taskID := model.TaskFragmentIdentifier{
		CreatorId:          taskIdentifier.GetCreatorId(),
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	if partialData.Ready {
		log.Infof("Task fragment %v is ready to delete for clientId: %s", taskID, clientId)
		return
	}

	if _, exists := partialData.TaskFragments[taskID]; exists {
		log.Infof("Task fragment %v already processed for clientId: %s", taskID, clientId)
		return
	}

	partialData.TaskFragments[taskID] = common.FragmentStatus{
		Logged: false,
	}

	taskNumber := int(taskID.TaskNumber)

	if _, exists := partialData.Data[taskNumber]; !exists {
		partialData.Data[taskNumber] = make(map[string][]B)
	}

	for _, item := range data {
		id := identifierFunc(item)

		if _, exists := partialData.Data[taskNumber][id]; !exists {
			partialData.Data[taskNumber][id] = make([]B, 0)
		}

		partialData.Data[taskNumber][id] = append(partialData.Data[taskNumber][id], mappingFunc(item))
	}
}

func (j *Joiner) LoadData(dirBase string) error {
	log.Infof("[Joiner] LoadData: cargando partialResults desde %s", dirBase)
	var err error
	j.partialResults, err = storage.LoadJoinerPartialResultsFromDisk(dirBase)
	if err != nil {
		log.Errorf("[Joiner] LoadData: error al cargar partialResults: %v", err)
		return err
	}
	return nil
}

func keysOfJoinerPartialResults(m map[string]*common.JoinerPartialResults) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (j *Joiner) SaveData(task *protocol.Task) error {
	stage := task.GetStage()
	tableType := task.GetTableType()
	clientId := task.GetClientId()
	taskIdentifier := task.GetTaskIdentifier()

	taskID := &model.TaskFragmentIdentifier{
		CreatorId:          taskIdentifier.GetCreatorId(),
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	switch s := stage.(type) {
	case *protocol.Task_Iota:

		IotaPartialData := j.partialResults[clientId].IotaData
		err := saveData(
			j.infraConfig.GetDirectory(),
			clientId,
			common.IOTA_STAGE,
			common.FolderType(tableType),
			IotaPartialData,
			taskID,
		)

		if err != nil {
			return fmt.Errorf("failed to save Iota data: %w", err)
		}

	case *protocol.Task_Zeta:

		ZetaPartialData := j.partialResults[clientId].ZetaData
		err := saveData(
			j.infraConfig.GetDirectory(),

			clientId,
			common.ZETA_STAGE,
			common.FolderType(tableType),
			ZetaPartialData,
			taskID,
		)
		if err != nil {
			return fmt.Errorf("failed to save Zeta data: %w", err)
		}

	case *protocol.Task_OmegaEOF:
		processedTableType := task.GetOmegaEOF().GetData().GetEofType()
		processedStage := task.GetOmegaEOF().GetData().GetStage()

		switch processedStage {
		case common.IOTA_STAGE:
			IotaPartialData := j.partialResults[clientId].IotaData
			err := saveData(
				j.infraConfig.GetDirectory(),
				clientId,
				processedStage,
				common.FolderType(processedTableType),
				IotaPartialData,
				nil,
			)

			if err != nil {
				return fmt.Errorf("failed to save OmegaEOF Iota data: %w", err)
			}

		case common.ZETA_STAGE:
			ZetaPartialData := j.partialResults[clientId].ZetaData
			err := saveData(
				j.infraConfig.GetDirectory(),
				clientId,
				processedStage,
				common.FolderType(processedTableType),
				ZetaPartialData,
				nil,
			)

			if err != nil {
				return fmt.Errorf("failed to save OmegaEOF Zeta data: %w", err)
			}
		default:
			return fmt.Errorf("invalid OmegaEOF stage: %s", processedStage)
		}

	case *protocol.Task_RingEOF:
		processedTableType := task.GetRingEOF().GetEofType()
		processedStage := task.GetRingEOF().GetStage()

		switch processedStage {
		case common.IOTA_STAGE:
			IotaPartialData := j.partialResults[clientId].IotaData
			err := saveData(
				j.infraConfig.GetDirectory(),
				clientId,
				processedStage,
				common.FolderType(processedTableType),
				IotaPartialData,
				nil,
			)

			if err != nil {
				return fmt.Errorf("failed to save OmegaEOF Iota data: %w", err)
			}

		case common.ZETA_STAGE:
			ZetaPartialData := j.partialResults[clientId].ZetaData
			err := saveData(
				j.infraConfig.GetDirectory(),
				clientId,
				processedStage,
				common.FolderType(processedTableType),
				ZetaPartialData,
				nil,
			)

			if err != nil {
				return fmt.Errorf("failed to save OmegaEOF Zeta data: %w", err)
			}
		default:
			return fmt.Errorf("invalid OmegaEOF stage: %s", processedStage)
		}

	default:
		return fmt.Errorf("invalid query stage: %v", s)
	}

	return nil
}

func saveData[S, B proto.Message](dirBase string, clientId string, stage string, tableType common.FolderType, stageData *common.JoinerStageData[S, B], taskIdentifier *model.TaskFragmentIdentifier) error {

	switch tableType {
	case common.JOINER_SMALL_FOLDER_TYPE:
		err := storage.SaveJoinerCompleteDataToFile(
			dirBase,
			clientId,
			stage,
			tableType,
			stageData,
			taskIdentifier)

		if err != nil {
			return fmt.Errorf("failed to save Joiner small table data: %w", err)
		}
		if taskIdentifier != nil {
			stageData.SmallTable.TaskFragments[*taskIdentifier] = common.FragmentStatus{
				Logged: true,
			}
		}

	case common.JOINER_BIG_FOLDER_TYPE:
		err := storage.SaveJoinerCompleteDataToFile(
			dirBase,
			clientId,
			stage,
			tableType,
			stageData,
			taskIdentifier,
		)

		if err != nil {
			return fmt.Errorf("failed to save Joiner big table data: %w", err)
		}

		if taskIdentifier != nil {
			stageData.BigTable.TaskFragments[*taskIdentifier] = common.FragmentStatus{
				Logged: true,
			}

		}

	default:
		return fmt.Errorf("Invalid table type: %s", tableType)
	}

	return nil
}

func tryDeleteBothTables[S, B proto.Message](dir, clientId, stage string, stageData *common.JoinerStageData[S, B]) error {
	log.Debugf("Deleting Iota data for clientId: %s, stage: %s", clientId, stage)

	err := storage.TryDeleteSmallTable(dir, stage, clientId, stageData.SmallTable.IsReadyToDelete, stageData.SmallTable.Ready, stageData)
	if err != nil {
		return fmt.Errorf("failed to delete Iota data small table: %w", err)
	}

	err = storage.TryDeleteBigTable(dir, stage, clientId, stageData.BigTable.IsReadyToDelete, stageData.BigTable.Ready, stageData)
	if err != nil {
		return fmt.Errorf("failed to delete Iota data: %w", err)
	}

	return nil
}

func (j *Joiner) DeleteData(task *protocol.Task) error {
	stage := task.GetStage()
	clientId := task.GetClientId()

	//log.Debugf("Deleting data for clientId: %s, stage: %s, tableType: %s", clientId, stage, tableType)

	iota := j.partialResults[clientId].IotaData
	zeta := j.partialResults[clientId].ZetaData

	dir := j.infraConfig.GetDirectory()

	switch s := stage.(type) {
	case *protocol.Task_Iota:
		return tryDeleteBothTables(dir, clientId, common.IOTA_STAGE, iota)

	case *protocol.Task_Zeta:
		return tryDeleteBothTables(dir, clientId, common.ZETA_STAGE, zeta)

	case *protocol.Task_OmegaEOF:
		processedStage := task.GetOmegaEOF().GetData().GetStage()
		switch processedStage {
		case common.IOTA_STAGE:
			return tryDeleteBothTables(dir, clientId, common.IOTA_STAGE, iota)

		case common.ZETA_STAGE:
			return tryDeleteBothTables(dir, clientId, common.ZETA_STAGE, zeta)

		default:
			return fmt.Errorf("invalid stage: %s", processedStage)
		}

	case *protocol.Task_RingEOF:
		processedStage := task.GetRingEOF().GetStage()
		switch processedStage {
		case common.IOTA_STAGE:
			return tryDeleteBothTables(dir, clientId, common.IOTA_STAGE, iota)

		case common.ZETA_STAGE:
			return tryDeleteBothTables(dir, clientId, common.ZETA_STAGE, zeta)

		default:
			return fmt.Errorf("invalid stage: %s", processedStage)
		}

	default:
		return fmt.Errorf("invalid query stage: %v", s)
	}

}
