package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils/topkheap"
	"github.com/op/go-logging"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const CLEANUP_INTERVAL = 10 * time.Minute

const VALID_STATUS = "valid"
const INVALID_STATUS = "invalid"

const FINAL_DATA_FILE_NAME = "data"
const FINAL_METADATA_FILE_NAME = "metadata"
const TEMPORARY_DATA_FILE_NAME = "data_temp"
const TEMPORARY_METADATA_FILE_NAME = "metadata_temp"
const FINAL_LOG_FILE_NAME = "logs"
const TEMPORARY_LOG_FILE_NAME = "logs_temp"
const JSON_FILE_EXTENSION = ".json"
const LOG_FILE_EXTENSION = ".log"

const MIN = "min"
const MAX = "max"

var log = logging.MustGetLogger("log")

type OutputFile struct {
	Data      interface{} `json:"data"`
	Timestamp string      `json:"timestamp"`
	Status    string      `json:"status"`
}

type MetadataFile struct {
	Timestamp      string `json:"timestamp"`
	Status         string `json:"status"`
	OmegaProcessed bool   `json:"omega_processed"`
	RingRound      uint32 `json:"ring_round"`
	IsReady        bool   `json:"is_ready"`
}

type JoinerOutputFile[T any] struct {
	Data                T      `json:"data"`
	Timestamp           string `json:"timestamp"`
	Status              string `json:"status"`
	OmegaProcessed      bool   `json:"omega_processed"`
	RingRound           uint32 `json:"ring_round"`
	SmallReadyToDelete  bool   `json:"small_ready_to_delete"`
	BigReadyToDelete    bool   `json:"big_ready_to_delete"`
	SendedTaskCount     int    `json:"sended_task_count"`
	SmallTableTaskCount int    `json:"small_table_task_count"`
	Ready               bool   `json:"ready"`
}

type MetaData struct {
	OmegaProcessed bool   `json:"omega_processed"`
	RingRound      uint32 `json:"ring_round"`
	IsReady        bool   `json:"is_ready"`
}

type JoinerMetaData struct {
	OmegaProcessed      bool   `json:"omega_processed"`
	RingRound           uint32 `json:"ring_round"`
	SendedTaskCount     int    `json:"sended_task_count"`
	SmallTableTaskCount int    `json:"small_table_task_count"`
	Ready               bool   `json:"ready"`
	SmallReadyToDelete  bool   `json:"small_ready_to_delete"`
	BigReadyToDelete    bool   `json:"big_ready_to_delete"`
}

// Generic loader for PartialData[T] using protojson
func decodeJsonToProtoData[T proto.Message](jsonBytes []byte, newT func() T) (*common.PartialData[T], string, error) {
	var temp struct {
		Data      map[string]json.RawMessage `json:"data"`
		Timestamp string                     `json:"timestamp"`
		Status    string                     `json:"status"`
	}
	if err := json.Unmarshal(jsonBytes, &temp); err != nil {
		return &common.PartialData[T]{Data: make(map[string]T)}, "", err
	}
	result := &common.PartialData[T]{
		Data: make(map[string]T),
	}
	for k, raw := range temp.Data {
		msg := newT()
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return result, "", err
		}
		result.Data[k] = msg
	}

	return result, temp.Timestamp, nil
}

func decodeGeneralFromJsonMetadata(jsonBytes []byte) (MetadataFile, error) {
	var temp MetadataFile
	if err := json.Unmarshal(jsonBytes, &temp); err != nil {
		return MetadataFile{}, err
	}
	return MetadataFile{
		Timestamp:      temp.Timestamp,
		Status:         temp.Status,
		OmegaProcessed: temp.OmegaProcessed,
		RingRound:      temp.RingRound,
		IsReady:        temp.IsReady,
	}, nil
}

func decodeJsonToJoinerBigTableData[S, B proto.Message](
	stageData *common.JoinerStageData[S, B],
	jsonBytes []byte,
	newB func() B,
	smallTableCommitTimestamp string,
) (string, error) {

	var temp JoinerOutputFile[map[int]map[string][]json.RawMessage]

	if err := json.Unmarshal(jsonBytes, &temp); err != nil {
		return "", err
	}

	for outerKey, innerMap := range temp.Data {
		stageData.BigTable.Data[outerKey] = make(map[string][]B)
		for innerKey, rawSlice := range innerMap {
			for _, raw := range rawSlice {
				msg := newB()
				if err := protojson.Unmarshal(raw, msg); err != nil {
					return temp.Timestamp, err
				}
				stageData.BigTable.Data[outerKey][innerKey] = append(stageData.BigTable.Data[outerKey][innerKey], msg)
			}
		}
	}

	stageData.RingRound = temp.RingRound
	stageData.BigTable.OmegaProcessed = temp.OmegaProcessed
	stageData.BigTable.Ready = temp.Ready

	tSmall, smallErr := time.Parse(time.RFC3339Nano, smallTableCommitTimestamp)
	tBig, bigErr := time.Parse(time.RFC3339Nano, temp.Timestamp)

	if bigErr != nil {
		return temp.Timestamp, fmt.Errorf("error parsing big table timestamp: %w", bigErr)
	}

	if smallErr != nil || tBig.After(tSmall) {
		stageData.BigTable.IsReadyToDelete = temp.BigReadyToDelete
		stageData.SmallTable.IsReadyToDelete = temp.SmallReadyToDelete
		stageData.SendedTaskCount = temp.SendedTaskCount
	}

	return temp.Timestamp, nil
}

func decodeJsonToJoinerSmallTableData[S, B proto.Message](
	stageData *common.JoinerStageData[S, B],
	jsonBytes []byte,
	newS func() S,
) (string, error) {

	var temp JoinerOutputFile[map[string]json.RawMessage]

	if err := json.Unmarshal(jsonBytes, &temp); err != nil {
		return "", err
	}

	for k, raw := range temp.Data {
		msg := newS()
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return temp.Timestamp, err
		}
		stageData.SmallTable.Data[k] = msg
	}

	stageData.SmallTableTaskCount = temp.SmallTableTaskCount
	stageData.SmallTable.OmegaProcessed = temp.OmegaProcessed
	stageData.SmallTable.Ready = temp.Ready

	stageData.BigTable.IsReadyToDelete = temp.BigReadyToDelete
	stageData.SmallTable.IsReadyToDelete = temp.SmallReadyToDelete
	stageData.SendedTaskCount = temp.SendedTaskCount

	return temp.Timestamp, nil
}

// Generic disk loader for any PartialData[T]
func loadStageClientData[T proto.Message](
	dir string, stage string, fileType common.FolderType, clientId string, newT func() T,
) (*common.PartialData[T], error) {

	log.Debugf("Loading data for stage %s, client %s", stage, clientId)

	partial := &common.PartialData[T]{
		Data:          make(map[string]T),
		TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
	}

	dirPath := filepath.Join(dir, stage, clientId)
	finalDataFilePath := filepath.Join(dirPath, FINAL_DATA_FILE_NAME+JSON_FILE_EXTENSION)
	finalMetadataFilePath := filepath.Join(dirPath, FINAL_METADATA_FILE_NAME+JSON_FILE_EXTENSION)

	err := cleanAllTempFiles(dirPath)
	if err != nil {
		log.Errorf("error cleaning temp files for stage %s, client %s: %v", stage, clientId, err)
	}

	log.Debugf("Temp files cleaned for stage %s, client %s", stage, clientId)

	// Read metadata file
	jsonMetadataBytes, err := os.ReadFile(finalMetadataFilePath)
	if err != nil {
		log.Warningf("error reading metadata file %s: %v", finalMetadataFilePath, err)
	} else {
		// tengo la metadata
		metadata, err := decodeGeneralFromJsonMetadata(jsonMetadataBytes)
		if err != nil {
			return partial, err
		}
		log.Debugf("Metadata loaded for stage %s, client %s: %+v", stage, clientId, metadata)

		partial.OmegaProcessed = metadata.OmegaProcessed
		partial.RingRound = metadata.RingRound
		partial.IsReady = metadata.IsReady

		if partial.IsReady {
			err := TryDeletePartialData(dirPath, stage, string(fileType), clientId, partial.IsReady)
			if err != nil {
				log.Errorf("error deleting partial data for stage %s, client %s: %v", stage, clientId, err)
			}

			log.Debugf("Partial data deleted for stage %s, client %s", stage, clientId)
			log.Infof("Stage %s for client %s is ready, skipping data loading", stage, clientId)

			return partial, nil
		}
	}

	//Read data file

	log.Debugf("Reading data file %s for stage %s, client %s", finalDataFilePath, stage, clientId)

	jsonDataBytes, err := os.ReadFile(finalDataFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Warningf("error reading file %s: %v", finalDataFilePath, err)
		} else {
			log.Errorf("error reading file %s: %v", finalDataFilePath, err)
			return partial, fmt.Errorf("error reading file %s: %w", finalDataFilePath, err)
		}
	} else {

		log.Debugf("Data file %s read successfully for stage %s, client %s", finalDataFilePath, stage, clientId)

		var timestamp string
		partialDecoded, partialTimestamp, err := decodeJsonToProtoData(jsonDataBytes, newT)
		if err != nil {
			log.Error("error decoding JSON data from file %s: %v", finalDataFilePath, err)
		}
		partial = partialDecoded
		timestamp = partialTimestamp

		log.Debugf("Loaded data for stage %s, client %s, timestamp: %s", stage, clientId, timestamp)

		taskFragments, err := readAndFilterTaskFragmentIdentifiersLogLiteral(dirPath, timestamp, stage)
		if err != nil {
			log.Warningf("error reading task fragments from log file: %w", err)
		}
		partial.TaskFragments = taskFragments
	}

	return partial, nil
}

// Generic stage loader for any PartialData[T] into any result struct R
func loadStageData[T proto.Message, R any](
	dir string,
	stage string,
	fileType common.FolderType,
	partialResults map[string]*R,
	setter func(result *R, data *common.PartialData[T]),
	newT func() T,
) {

	stageDir := filepath.Join(dir, stage)
	clientDirs, err := os.ReadDir(stageDir)
	if err != nil {
		log.Warningf("Error reading directory %s: %v", stageDir, err)
		return
	}

	for _, clientEntry := range clientDirs {
		if !clientEntry.IsDir() {
			continue
		}
		clientId := clientEntry.Name()
		data, err := loadStageClientData(dir, stage, fileType, clientId, newT)
		if err != nil {
			log.Warningf("Error loading data for stage %s, client %s: %v", stage, clientId, err)
			continue
		}
		if _, ok := partialResults[clientId]; !ok || partialResults[clientId] == nil {
			partialResults[clientId] = new(R)
		}
		setter(partialResults[clientId], data)
	}
}

func LoadMergerPartialResultsFromDisk(dir string) (map[string]*common.MergerPartialResults, error) {
	partialResults := make(map[string]*common.MergerPartialResults)
	folderType := common.GENERAL_FOLDER_TYPE

	// Enumerar clientIds presentes en los stages relevantes
	clientIdSet := make(map[string]struct{})
	stages := []string{common.DELTA_STAGE_3, common.ETA_STAGE_3, common.KAPPA_STAGE_3, common.NU_STAGE_3}
	for _, stage := range stages {
		stageDir := filepath.Join(dir, stage, string(folderType))
		clientDirs, err := os.ReadDir(stageDir)
		if err != nil {
			continue
		}
		for _, entry := range clientDirs {
			if entry.IsDir() {
				clientIdSet[entry.Name()] = struct{}{}
			}
		}
	}
	// Inicializar estructura para cada clientId
	for clientId := range clientIdSet {
		partialResults[clientId] = &common.MergerPartialResults{
			Delta3: &common.PartialData[*protocol.Delta_3_Data]{
				Data:          make(map[string]*protocol.Delta_3_Data),
				TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
			},
			Eta3: &common.PartialData[*protocol.Eta_3_Data]{
				Data:          make(map[string]*protocol.Eta_3_Data),
				TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
			},
			Kappa3: &common.PartialData[*protocol.Kappa_3_Data]{
				Data:          make(map[string]*protocol.Kappa_3_Data),
				TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
			},
			Nu3: &common.PartialData[*protocol.Nu_3_Data]{
				Data:          make(map[string]*protocol.Nu_3_Data),
				TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
			},
		}
	}

	loadStageData(dir, common.DELTA_STAGE_3, folderType, partialResults, func(result *common.MergerPartialResults, data *common.PartialData[*protocol.Delta_3_Data]) {
		result.Delta3 = data
	}, func() *protocol.Delta_3_Data { return &protocol.Delta_3_Data{} })

	loadStageData(dir, common.ETA_STAGE_3, folderType, partialResults, func(result *common.MergerPartialResults, data *common.PartialData[*protocol.Eta_3_Data]) {
		result.Eta3 = data
	}, func() *protocol.Eta_3_Data { return &protocol.Eta_3_Data{} })

	loadStageData(dir, common.KAPPA_STAGE_3, folderType, partialResults, func(result *common.MergerPartialResults, data *common.PartialData[*protocol.Kappa_3_Data]) {
		result.Kappa3 = data
	}, func() *protocol.Kappa_3_Data { return &protocol.Kappa_3_Data{} })

	loadStageData(dir, common.NU_STAGE_3, folderType, partialResults, func(result *common.MergerPartialResults, data *common.PartialData[*protocol.Nu_3_Data]) {
		result.Nu3 = data
	}, func() *protocol.Nu_3_Data { return &protocol.Nu_3_Data{} })

	return partialResults, nil
}

func LoadReducerPartialResultsFromDisk(dir string) (map[string]*common.ReducerPartialResults, error) {
	partialResults := make(map[string]*common.ReducerPartialResults)
	fileType := common.GENERAL_FOLDER_TYPE

	clientIdSet := make(map[string]struct{})
	stages := []string{common.DELTA_STAGE_2, common.ETA_STAGE_2, common.KAPPA_STAGE_2, common.NU_STAGE_2}
	for _, stage := range stages {
		stageDir := filepath.Join(dir, stage, string(fileType))
		clientDirs, err := os.ReadDir(stageDir)
		if err != nil {
			continue
		}
		for _, entry := range clientDirs {
			if entry.IsDir() {
				clientIdSet[entry.Name()] = struct{}{}
			}
		}
	}
	for clientId := range clientIdSet {
		partialResults[clientId] = &common.ReducerPartialResults{
			Delta2: &common.PartialData[*protocol.Delta_2_Data]{
				Data:          make(map[string]*protocol.Delta_2_Data),
				TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
			},
			Eta2: &common.PartialData[*protocol.Eta_2_Data]{
				Data:          make(map[string]*protocol.Eta_2_Data),
				TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
			},
			Kappa2: &common.PartialData[*protocol.Kappa_2_Data]{
				Data:          make(map[string]*protocol.Kappa_2_Data),
				TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
			},
			Nu2: &common.PartialData[*protocol.Nu_2_Data]{
				Data:          make(map[string]*protocol.Nu_2_Data),
				TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
			},
		}
	}

	loadStageData(dir, common.DELTA_STAGE_2, fileType, partialResults, func(result *common.ReducerPartialResults, data *common.PartialData[*protocol.Delta_2_Data]) {
		result.Delta2 = data
	}, func() *protocol.Delta_2_Data { return &protocol.Delta_2_Data{} })

	loadStageData(dir, common.ETA_STAGE_2, fileType, partialResults, func(result *common.ReducerPartialResults, data *common.PartialData[*protocol.Eta_2_Data]) {
		result.Eta2 = data
	}, func() *protocol.Eta_2_Data { return &protocol.Eta_2_Data{} })

	loadStageData(dir, common.KAPPA_STAGE_2, fileType, partialResults, func(result *common.ReducerPartialResults, data *common.PartialData[*protocol.Kappa_2_Data]) {
		result.Kappa2 = data
	}, func() *protocol.Kappa_2_Data { return &protocol.Kappa_2_Data{} })

	loadStageData(dir, common.NU_STAGE_2, fileType, partialResults, func(result *common.ReducerPartialResults, data *common.PartialData[*protocol.Nu_2_Data]) {
		result.Nu2 = data
	}, func() *protocol.Nu_2_Data { return &protocol.Nu_2_Data{} })

	return partialResults, nil
}

func LoadTopperPartialResultsFromDisk(dir string) (map[string]*common.TopperPartialResults, error) {
	partialResults := make(map[string]*common.TopperPartialResults)
	folderType := common.GENERAL_FOLDER_TYPE

	clientIdSet := make(map[string]struct{})
	stages := []string{common.EPSILON_STAGE, common.LAMBDA_STAGE, common.THETA_STAGE}
	for _, stage := range stages {
		stageDir := filepath.Join(dir, stage, string(folderType))
		clientDirs, err := os.ReadDir(stageDir)
		if err != nil {
			continue
		}
		for _, entry := range clientDirs {
			if entry.IsDir() {
				clientIdSet[entry.Name()] = struct{}{}
			}
		}
	}
	for clientId := range clientIdSet {
		partialResults[clientId] = &common.TopperPartialResults{
			EpsilonData: &common.TopperPartialData[uint64, *protocol.Epsilon_Data]{
				Heap:          topkheap.NewTopKMaxHeap[uint64, *protocol.Epsilon_Data](0),
				TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
			},
			LamdaData: &common.TopperPartialData[uint64, *protocol.Lambda_Data]{
				Heap:          topkheap.NewTopKMaxHeap[uint64, *protocol.Lambda_Data](0),
				TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
			},
			ThetaData: &common.ThetaPartialData{
				MinPartialData: &common.TopperPartialData[float32, *protocol.Theta_Data]{
					Heap:          topkheap.NewTopKMinHeap[float32, *protocol.Theta_Data](1),
					TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
				},
				MaxPartialData: &common.TopperPartialData[float32, *protocol.Theta_Data]{
					Heap:          topkheap.NewTopKMaxHeap[float32, *protocol.Theta_Data](1),
					TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
				},
			},
		}
	}

	loadStageData(dir, common.EPSILON_STAGE, folderType, partialResults,
		func(result *common.TopperPartialResults, data *common.PartialData[*protocol.Epsilon_Data]) {
			if result.EpsilonData == nil {
				result.EpsilonData = &common.TopperPartialData[uint64, *protocol.Epsilon_Data]{}
			}
			result.EpsilonData.OmegaProcessed = data.OmegaProcessed
			result.EpsilonData.RingRound = data.RingRound
			result.EpsilonData.Heap = partialDatatoHeap(common.TYPE_MAX, data.Data,
				func(item *protocol.Epsilon_Data) uint64 {
					return item.GetTotalInvestment()
				})
		},
		func() *protocol.Epsilon_Data { return &protocol.Epsilon_Data{} })

	loadStageData(dir, common.LAMBDA_STAGE, folderType, partialResults,
		func(result *common.TopperPartialResults, data *common.PartialData[*protocol.Lambda_Data]) {
			if result.LamdaData == nil {
				result.LamdaData = &common.TopperPartialData[uint64, *protocol.Lambda_Data]{}
			}
			result.LamdaData.RingRound = data.RingRound
			result.LamdaData.OmegaProcessed = data.OmegaProcessed
			result.LamdaData.Heap = partialDatatoHeap(common.TYPE_MAX, data.Data,
				func(item *protocol.Lambda_Data) uint64 {
					return item.GetParticipations()
				})
		},
		func() *protocol.Lambda_Data { return &protocol.Lambda_Data{} })

	loadStageData(dir, common.THETA_STAGE, folderType, partialResults,
		func(result *common.TopperPartialResults, data *common.PartialData[*protocol.Theta_Data]) {
			if result.ThetaData == nil {
				result.ThetaData = &common.ThetaPartialData{
					MinPartialData: &common.TopperPartialData[float32, *protocol.Theta_Data]{},
					MaxPartialData: &common.TopperPartialData[float32, *protocol.Theta_Data]{},
				}
			}
			result.ThetaData.MinPartialData.OmegaProcessed = data.OmegaProcessed
			result.ThetaData.MinPartialData.RingRound = data.RingRound
			result.ThetaData.MaxPartialData.OmegaProcessed = data.OmegaProcessed
			result.ThetaData.MaxPartialData.RingRound = data.RingRound
			result.ThetaData.MaxPartialData.Heap = maxToHeap(data.Data[MAX],
				func(item *protocol.Theta_Data) float32 {
					return item.GetAvgRating()
				})
			result.ThetaData.MinPartialData.Heap = minToHeap(data.Data[MIN],
				func(item *protocol.Theta_Data) float32 {
					return item.GetAvgRating()
				})
		},
		func() *protocol.Theta_Data { return &protocol.Theta_Data{} })

	return partialResults, nil
}

func LoadJoinerPartialResultsFromDisk(
	dir string,
) (map[string]*common.JoinerPartialResults, error) {
	log.Infof("[Storage] LoadJoinerPartialResultsFromDisk: cargando desde %s", dir)
	partialResults := make(map[string]*common.JoinerPartialResults)

	clientIdSet := make(map[string]struct{})
	stages := []string{common.IOTA_STAGE, common.ZETA_STAGE}
	folders := []string{string(common.JOINER_SMALL_FOLDER_TYPE), string(common.JOINER_BIG_FOLDER_TYPE)}
	for _, stage := range stages {
		for _, folder := range folders {
			stageDir := filepath.Join(dir, stage, folder)
			clientDirs, err := os.ReadDir(stageDir)
			if err != nil {
				continue
			}
			for _, entry := range clientDirs {
				if entry.IsDir() {
					clientIdSet[entry.Name()] = struct{}{}
				}
			}
		}
	}

	// 2. Inicializar partialResults para cada clientId
	for clientId := range clientIdSet {
		clientPartialResults := &common.JoinerPartialResults{
			IotaData: &common.JoinerStageData[*protocol.Iota_Data_Movie, *protocol.Iota_Data_Actor]{
				SmallTable: &common.JoinerTableData[common.SmallTableData[*protocol.Iota_Data_Movie]]{
					Data:          make(common.SmallTableData[*protocol.Iota_Data_Movie]),
					TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
				},
				BigTable: &common.JoinerTableData[common.BigTableData[*protocol.Iota_Data_Actor]]{
					Data:          make(common.BigTableData[*protocol.Iota_Data_Actor]),
					TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
				},
			},
			ZetaData: &common.JoinerStageData[*protocol.Zeta_Data_Movie, *protocol.Zeta_Data_Rating]{
				SmallTable: &common.JoinerTableData[common.SmallTableData[*protocol.Zeta_Data_Movie]]{
					Data:          make(common.SmallTableData[*protocol.Zeta_Data_Movie]),
					TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
				},
				BigTable: &common.JoinerTableData[common.BigTableData[*protocol.Zeta_Data_Rating]]{
					Data:          make(common.BigTableData[*protocol.Zeta_Data_Rating]),
					TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
				},
			},
		}
		err := loadJoinerStageToPartialResults(
			dir,
			common.IOTA_STAGE,
			clientId,
			func() *protocol.Iota_Data_Movie { return &protocol.Iota_Data_Movie{} },
			func() *protocol.Iota_Data_Actor { return &protocol.Iota_Data_Actor{} },
			clientPartialResults.IotaData,
		)

		if err != nil {
			log.Errorf("[Storage] LoadJoinerPartialResultsFromDisk: error loading IOTA: %v", err)
			return nil, fmt.Errorf("error loading IOTA joiner stage: %w", err)
		}

		err = loadJoinerStageToPartialResults(
			dir,
			common.ZETA_STAGE,
			clientId,
			func() *protocol.Zeta_Data_Movie { return &protocol.Zeta_Data_Movie{} },
			func() *protocol.Zeta_Data_Rating { return &protocol.Zeta_Data_Rating{} },
			clientPartialResults.ZetaData,
		)

		if err != nil {
			log.Errorf("[Storage] LoadJoinerPartialResultsFromDisk: error loading ZETA: %v", err)
			return nil, fmt.Errorf("error loading ZETA joiner stage: %w", err)
		}

		partialResults[clientId] = clientPartialResults

	}

	log.Infof("[Storage] LoadJoinerPartialResultsFromDisk: partialResults keys=%v", keysOfJoinerPartialResults(partialResults))
	return partialResults, nil

}

// Esta función carga los datos de un stage y los setea en el campo correspondiente de JoinerPartialResults
func loadJoinerStageToPartialResults[S proto.Message, B proto.Message](
	dir string,
	stage string,
	clientId string,
	newS func() S,
	newB func() B,
	stageData *common.JoinerStageData[S, B],
) error {
	log.Infof("[Storage] loadJoinerStageToPartialResults: stage=%s", stage)

	// 1. Limpio archivos temporales

	smallTableDir := filepath.Join(dir, stage, string(common.JOINER_SMALL_FOLDER_TYPE), clientId)
	bigTableDir := filepath.Join(dir, stage, string(common.JOINER_BIG_FOLDER_TYPE), clientId)

	err := cleanAllTempFiles(smallTableDir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Warningf("No temporary files to clean for client %s in stage %s",
				clientId, stage)
		} else {

			log.Errorf("Error cleaning temporary files for client %s in stage %s: %v ", clientId, stage, err)
			return fmt.Errorf("error cleaning small table temp files for client %s: %w", clientId, err)
		}
	}

	err = cleanAllTempFiles(bigTableDir)
	if err != nil {
		if os.IsNotExist(err) {
			log.Warningf("No temporary files to clean for client %s in stage %s",
				clientId, stage)
		} else {
			log.Errorf("Error cleaning temporary files for client %s in stage %s: %v", clientId, stage, err)
			return fmt.Errorf("error cleaning big table temp files for client %s: %w", clientId, err)
		}
	}

	// 2. Leo los metadatos de cada tabla (small y big)

	smallDataPath := filepath.Join(smallTableDir, FINAL_DATA_FILE_NAME+JSON_FILE_EXTENSION)
	bigDataPath := filepath.Join(bigTableDir, FINAL_DATA_FILE_NAME+JSON_FILE_EXTENSION)

	smallTableData, err := os.ReadFile(smallDataPath)

	var smallTableTimestamp string
	var bigTableTimestamp string

	if err != nil {
		log.Warningf("There is no small metadata for client %s in stage %s, using default values", clientId, stage)
	} else {
		smallTableTimestamp, err = decodeJsonToJoinerSmallTableData(stageData, smallTableData, newS)

		if err != nil {
			log.Warningf("Error decoding metadata for client %s: %v", clientId, err)
		}
	}

	bigTableData, err := os.ReadFile(bigDataPath)

	if err != nil {
		log.Warningf("There is no big metadata for client %s in stage %s, using default values", clientId, stage)
	} else {
		bigTableTimestamp, err = decodeJsonToJoinerBigTableData(stageData, bigTableData, newB, smallTableTimestamp)
		if err != nil {
			log.Errorf("Error decoding metadata for client %s: %v", clientId, err)
		}
	}

	log.Debugf("	=%d, smallTableTaskCount=%d, smallReadyToDelete=%v, bigReadyToDelete=%v",
		stageData.SendedTaskCount,
		stageData.SmallTableTaskCount,
		stageData.SmallTable.IsReadyToDelete,
		stageData.BigTable.IsReadyToDelete)

	// 3. Leo los task fragments de cada tabla (small y big)

	if smallTableTimestamp != "" && !stageData.SmallTable.Ready {
		smallTableTaskFragments, err := readAndFilterTaskFragmentIdentifiersLogLiteral(smallTableDir, smallTableTimestamp, stage)
		if err != nil {
			log.Errorf("error reading task fragments from log file: %w", err)
			return err
		}

		stageData.SmallTable.TaskFragments = smallTableTaskFragments
	}

	if bigTableTimestamp != "" && !stageData.BigTable.Ready {
		bigTableTaskFragments, err := readAndFilterTaskFragmentIdentifiersLogLiteral(bigTableDir, bigTableTimestamp, stage)
		if err != nil {
			log.Errorf("error reading task fragments from log file: %w", err)
			return err
		}

		stageData.BigTable.TaskFragments = bigTableTaskFragments
	}

	// 4. Borro los archivos que ya no necesito

	err = TryDeleteSmallTable(dir, stage, clientId, stageData.SmallTable.IsReadyToDelete, stageData.SmallTable.Ready, stageData)
	if err != nil {
		log.Errorf("Error deleting small table data for client %s, stage %s: %v", clientId, stage, err)
		return err
	}

	err = TryDeleteBigTable(dir, stage, clientId, stageData.BigTable.IsReadyToDelete, stageData.BigTable.Ready, stageData)
	if err != nil {
		log.Errorf("Error deleting big table data for client %s, stage %s: %v", clientId, stage, err)
		return err
	}

	return nil
}

func heapToPartialData[K topkheap.Ordered, V proto.Message](
	heap topkheap.TopKHeap[K, V],
	data map[string]V,
	keyFunc func(V) K,
) {

	for _, item := range heap.GetTopK() {
		key := keyFunc(item)
		data[fmt.Sprintf("%v", key)] = item
	}
}
func partialDatatoHeap[K topkheap.Ordered, V proto.Message](
	heapType string,
	data map[string]V,
	valueFunc func(V) K,
) topkheap.TopKHeap[K, V] {

	var heap topkheap.TopKHeap[K, V]

	if heapType == common.TYPE_MAX {
		heap = topkheap.NewTopKMaxHeap[K, V](len(data))
	} else {
		heap = topkheap.NewTopKMinHeap[K, V](len(data))
	}

	for _, item := range data {
		log.Infof("Adding item to heap: %v", item)
		heap.Insert(valueFunc(item), item)
	}

	return heap
}

func minToHeap[K topkheap.Ordered, V proto.Message](
	data V,
	keyFunc func(V) K,
) topkheap.TopKHeap[K, V] {
	heap := topkheap.NewTopKMinHeap[K, V](1)
	heap.Insert(keyFunc(data), data)
	return heap
}
func maxToHeap[K topkheap.Ordered, V proto.Message](
	data V,
	keyFunc func(V) K,
) topkheap.TopKHeap[K, V] {
	heap := topkheap.NewTopKMaxHeap[K, V](1)
	heap.Insert(keyFunc(data), data)
	return heap
}

func DeletePartialResults(dir string, stage interface{}, tableType string, clientId string) error {
	stringStage, err := getStageNameFromInterface(stage)
	if err != nil {
		return fmt.Errorf("error getting stage name: %w", err)
	}
	dirPath := filepath.Join(dir, stringStage, tableType, clientId)
	return os.RemoveAll(dirPath)
}

func TryDeletePartialData(dir string, stage string, folderType string, clientId string, deleteData bool) error {
	dirPath := filepath.Join(dir, stage, clientId)
	dataFilePath := filepath.Join(dirPath, FINAL_DATA_FILE_NAME+JSON_FILE_EXTENSION)
	logsFilePath := filepath.Join(dirPath, FINAL_LOG_FILE_NAME+LOG_FILE_EXTENSION)

	if deleteData {
		err := os.Remove(dataFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				log.Debugf("Data file %s does not exist, skipping deletion", dataFilePath)
				return nil
			}
			return fmt.Errorf("error deleting partial data for stage %s, folder type %s, client ID %s: %w", stage, folderType, clientId, err)
		}
		err = os.Remove(logsFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				log.Debugf("Logs file %s does not exist, skipping deletion", logsFilePath)
				return nil
			}
			return fmt.Errorf("error deleting partial data for stage %s, folder type %s, client ID %s: %w", stage, folderType, clientId, err)
		}

		log.Infof("Deleted data AND logs file %s", dataFilePath)
	}

	return nil
}

func TryDeleteSmallTable[S, B proto.Message](dir string, stage string, clientId string, deleteData bool, deleteLogs bool, stageData *common.JoinerStageData[S, B]) error {
	baseDir := filepath.Join(dir, stage, string(common.JOINER_SMALL_FOLDER_TYPE), clientId)

	if deleteData {
		stageData.SmallTable.Data = make(common.SmallTableData[S])

		marshaler := protojson.MarshalOptions{
			Indent:          "  ",
			EmitUnpopulated: true,
		}

		timestamp := time.Now().UTC().Format(time.RFC3339Nano)

		err := processJoinerSmallTableData(stageData, baseDir, marshaler, timestamp)
		if err != nil {
			return fmt.Errorf("error processing small table data for stage %s, folder type %s, client ID %s: %w", stage, common.JOINER_SMALL_FOLDER_TYPE, clientId, err)
		}

		commitPartialDataToFinal(dir, stage, common.JOINER_SMALL_FOLDER_TYPE, clientId)
	}

	if deleteLogs {
		logsFilePath := filepath.Join(baseDir, FINAL_LOG_FILE_NAME+LOG_FILE_EXTENSION)
		err := os.Remove(logsFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				log.Debugf("Logs file %s does not exist, skipping deletion", logsFilePath)
				return nil
			}
			return fmt.Errorf("error deleting logs for stage %s, folder type %s, client ID %s: %w", stage, common.JOINER_SMALL_FOLDER_TYPE, clientId, err)
		}

		log.Infof("Deleted logs file %s", logsFilePath)
	}

	return nil
}

func TryDeleteBigTable[S, B proto.Message](dir string, stage string, clientId string, deleteData bool, deleteLogs bool, stageData *common.JoinerStageData[S, B]) error {
	baseDir := filepath.Join(dir, stage, string(common.JOINER_BIG_FOLDER_TYPE), clientId)

	if deleteData {
		stageData.BigTable.Data = make(common.BigTableData[B])

		marshaler := protojson.MarshalOptions{
			Indent:          "  ",
			EmitUnpopulated: true,
		}

		timestamp := time.Now().UTC().Format(time.RFC3339Nano)

		err := processJoinerBigTableData(stageData, baseDir, marshaler, timestamp)
		if err != nil {
			return fmt.Errorf("error processing big table data for stage %s, folder type %s, client ID %s: %w", stage, common.JOINER_BIG_FOLDER_TYPE, clientId, err)
		}

		commitPartialDataToFinal(dir, stage, common.JOINER_BIG_FOLDER_TYPE, clientId)
	}

	if deleteLogs {
		logsFilePath := filepath.Join(baseDir, FINAL_LOG_FILE_NAME+LOG_FILE_EXTENSION)
		err := os.Remove(logsFilePath)
		if err != nil {
			if os.IsNotExist(err) {
				log.Debugf("Logs file %s does not exist, skipping deletion", logsFilePath)
				return nil
			}
			return fmt.Errorf("error deleting logs for stage %s, folder type %s, client ID %s: %w", stage, common.JOINER_BIG_FOLDER_TYPE, clientId, err)
		}

		log.Infof("Deleted logs file %s", logsFilePath)
	}

	return nil
}

func commitPartialMetadataToFinal(dirPath string) error {
	log.Infof("Committing metadata for path %s", dirPath)

	tempMetadataFileName := TEMPORARY_METADATA_FILE_NAME + JSON_FILE_EXTENSION
	finalMetadataFileName := FINAL_METADATA_FILE_NAME + JSON_FILE_EXTENSION

	return renameFile(dirPath, tempMetadataFileName, finalMetadataFileName)
}

func commitPartialDataToFinal(dir string, stage string, folderType common.FolderType, clientId string) error {

	if dir != "" {
		log.Infof("Committing data for stage %s, table type %s, client ID %s", stage, folderType, clientId)

		dirPath := filepath.Join(dir, stage, string(folderType), clientId)

		tempDataFileName := TEMPORARY_DATA_FILE_NAME + JSON_FILE_EXTENSION
		finalDataFileName := FINAL_DATA_FILE_NAME + JSON_FILE_EXTENSION

		return renameFile(dirPath, tempDataFileName, finalDataFileName)

	}

	return nil
}

func renameFile(dirPath, tempFileName, finalFileName string) error {
	tempDataFilePath := filepath.Join(dirPath, tempFileName)
	finalDataFilePath := filepath.Join(dirPath, finalFileName)

	log.Infof("Committing data from %s to %s", tempDataFilePath, finalDataFilePath)

	if err := os.Rename(tempDataFilePath, finalDataFilePath); err != nil {
		return fmt.Errorf("error renaming temp file: %w", err)
	}
	return nil
}

func SaveTopperThetaDataToFile[K topkheap.Ordered, T proto.Message](dir string, stage interface{}, clientId string, data []*common.TopperPartialData[K, T], taskFragment model.TaskFragmentIdentifier, keyFunc func(T) K) error {

	mapData := make(map[string]T)
	mapData[MAX] = data[0].Heap.GetTopK()[0]
	mapData[MIN] = data[1].Heap.GetTopK()[0]

	topperPartialData := &common.PartialData[T]{
		Data:           mapData,
		OmegaProcessed: data[0].OmegaProcessed,
		RingRound:      data[0].RingRound,
	}

	return SaveDataToFile(dir, clientId, stage, common.GENERAL_FOLDER_TYPE, topperPartialData, taskFragment)

}
func SaveTopperDataToFile[K topkheap.Ordered, T proto.Message](dir string, stage interface{}, clientId string, data *common.TopperPartialData[K, T], taskFragment model.TaskFragmentIdentifier, keyFunc func(T) K) error {

	mapData := make(map[string]T)
	heapToPartialData(data.Heap, mapData, keyFunc)

	topperPartialData := &common.PartialData[T]{
		Data:           mapData,
		OmegaProcessed: data.OmegaProcessed,
		RingRound:      data.RingRound,
	}

	return SaveDataToFile(dir, clientId, stage, common.GENERAL_FOLDER_TYPE, topperPartialData, taskFragment)

}
func SaveTopperMetadataToFile[K topkheap.Ordered, T proto.Message](dir string, clientId string, stage string, data *common.TopperPartialData[K, T]) error {

	dirPath := filepath.Join(dir, stage, clientId)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tempMetadataFilePath := filepath.Join(dirPath, TEMPORARY_METADATA_FILE_NAME+JSON_FILE_EXTENSION)

	metadata := MetadataFile{
		Timestamp:      time.Now().UTC().Format(time.RFC3339Nano),
		Status:         VALID_STATUS,
		OmegaProcessed: data.OmegaProcessed,
		RingRound:      data.RingRound,
		IsReady:        data.IsReady,
	}

	err = writeToTempFile(tempMetadataFilePath, metadata)
	if err != nil {
		return fmt.Errorf("failed to save data to temporary file: %w", err)
	}

	err = commitPartialMetadataToFinal(dirPath)
	if err != nil {
		return fmt.Errorf("failed to commit metadata to final file: %w", err)
	}
	return nil
}

func SaveDataToFile[T proto.Message](dir string, clientId string, stage interface{}, tableType common.FolderType, data *common.PartialData[T], taskFragment model.TaskFragmentIdentifier) error {
	stringStage, err := getStageNameFromInterface(stage)
	if err != nil {
		return fmt.Errorf("error getting stage name: %w", err)
	}

	timestamp := time.Now().UTC().Format(time.RFC3339Nano)

	//Tengo que retornar si falla ?

	log.Info("Task Fragment Identifier: ", taskFragment)
	if data.TaskFragments[taskFragment].Logged == false {
		log.Infof("Appending task fragment identifiers log literal for stage %s, table type %s, client ID %s, timestamp %s, task fragment %v", stringStage, tableType, clientId, timestamp, taskFragment)
		err = appendTaskFragmentIdentifiersLogLiteral(dir, clientId, stringStage, tableType, timestamp, taskFragment)
		if err != nil {
			return fmt.Errorf("error appending task fragment identifiers log literal: %w", err)
		}
	}

	err = saveGeneralDataToTempFile(dir, clientId, stringStage, tableType, timestamp, data)
	if err != nil {
		return fmt.Errorf("error saving general data to file: %w", err)
	}

	return commitPartialDataToFinal(dir, stringStage, tableType, clientId)
}
func SaveMetadataToFile[T proto.Message](dir string, clientId string, stage string, tableType common.FolderType, data *common.PartialData[T]) error {
	dirPath := filepath.Join(dir, stage, string(tableType), clientId)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tempMetadataFilePath := filepath.Join(dirPath, TEMPORARY_METADATA_FILE_NAME+JSON_FILE_EXTENSION)

	metadata := MetadataFile{
		Timestamp:      time.Now().UTC().Format(time.RFC3339Nano),
		Status:         VALID_STATUS,
		OmegaProcessed: data.OmegaProcessed,
		RingRound:      data.RingRound,
		IsReady:        data.IsReady,
	}

	err = writeToTempFile(tempMetadataFilePath, metadata)

	if err != nil {
		return fmt.Errorf("failed to save data to temporary file: %w", err)
	}

	err = commitPartialMetadataToFinal(dirPath)
	if err != nil {
		return fmt.Errorf("failed to commit metadata to final file: %w", err)
	}
	return nil
}
func saveGeneralDataToTempFile[T proto.Message](dir string, clientId string, stage string, tableType common.FolderType, timestamp string, data *common.PartialData[T]) error {

	dirPath := filepath.Join(dir, stage, string(tableType), clientId)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tempDataFilePath := filepath.Join(dirPath, TEMPORARY_DATA_FILE_NAME+JSON_FILE_EXTENSION)

	err = marshallGeneralPartialData(tempDataFilePath, timestamp, data)
	if err != nil {
		return fmt.Errorf("failed to save data to temporary file: %w", err)
	}

	return nil
}
func marshallGeneralPartialData[T proto.Message](tempDataFilePath string, timestamp string, data *common.PartialData[T]) error {

	marshaler := protojson.MarshalOptions{
		Indent:          "  ", // Pretty print
		EmitUnpopulated: true, // Include unpopulated fields
	}
	return processGeneralDataTypedStruct(data, tempDataFilePath, timestamp, marshaler)

}

func processGeneralDataTypedStruct[T proto.Message](typedStruct *common.PartialData[T], tempDataFilePath string, timestamp string, marshaler protojson.MarshalOptions) error {
	log.Infof("Processing data to download to file: %s", tempDataFilePath)

	jsonMap := make(map[string]json.RawMessage)
	for key, msg := range typedStruct.Data {
		marshaledData, err := marshaler.Marshal(msg)
		if err != nil {
			return fmt.Errorf("error marshaling data to JSON: %w", err)
		}
		jsonMap[key] = marshaledData
	}

	outputData := OutputFile{
		Data:      jsonMap,
		Timestamp: timestamp,
		Status:    VALID_STATUS,
	}

	if err := writeToTempFile(tempDataFilePath, outputData); err != nil {
		return fmt.Errorf("error writing to temp file: %w", err)
	}

	return nil
}

func SaveJoinerCompleteDataToFile[T proto.Message, B proto.Message](
	dir string,
	clientId string,
	stage string,
	tableType common.FolderType,
	data *common.JoinerStageData[T, B],
	taskFragment *model.TaskFragmentIdentifier,
) error {
	log.Infof("Saving joiner complete data to file for client %s, stage %s, table type %s", clientId, stage, tableType)
	dirPath := filepath.Join(dir, stage, string(tableType), clientId)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)

	}
	err = saveJoinerDataToFile(dir, clientId, stage, tableType, data, taskFragment)
	if err != nil {
		return fmt.Errorf("error saving joiner small table data to file: %w", err)
	}
	return commitPartialDataToFinal(dir, stage, tableType, clientId)

}

func saveJoinerDataToFile[S, B proto.Message](
	dir string,
	clientId string,
	stage string,
	tableType common.FolderType,
	data *common.JoinerStageData[S, B],
	taskFragment *model.TaskFragmentIdentifier,
) error {
	dirPath := filepath.Join(dir, stage, string(tableType), clientId)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	err = marshallJoinerAnyTableData(dirPath, data, taskFragment, tableType)
	if err != nil {
		return fmt.Errorf("failed to save data to temporary file: %w", err)
	}

	return nil
}

func marshallJoinerAnyTableData[S, B proto.Message](
	dirPath string,
	data *common.JoinerStageData[S, B],
	taskFragment *model.TaskFragmentIdentifier,
	tableType common.FolderType,
) error {
	marshaler := protojson.MarshalOptions{
		Indent:          "  ",
		EmitUnpopulated: true,
	}

	timestamp := time.Now().UTC().Format(time.RFC3339Nano)

	if tableType == common.JOINER_BIG_FOLDER_TYPE {

		err := processJoinerBigTableData(data, dirPath, marshaler, timestamp)

		if err != nil {
			return fmt.Errorf("error processing big table joiner data: %w", err)
		}

		if taskFragment != nil && !data.BigTable.TaskFragments[*taskFragment].Logged {
			err := appendTaskFragmentIdentifiersLogLiteralByPath(dirPath, timestamp, *taskFragment)
			if err != nil {
				return fmt.Errorf("error appending task fragment identifiers log literal: %w", err)
			}
		}

	} else if tableType == common.JOINER_SMALL_FOLDER_TYPE {

		err := processJoinerSmallTableData(data, dirPath, marshaler, timestamp)

		if err != nil {
			return fmt.Errorf("error processing small table joiner data: %w", err)
		}

		if taskFragment != nil && !data.SmallTable.TaskFragments[*taskFragment].Logged {
			err := appendTaskFragmentIdentifiersLogLiteralByPath(dirPath, timestamp, *taskFragment)
			if err != nil {
				return fmt.Errorf("error appending task fragment identifiers log literal: %w", err)
			}
		}
	}

	return nil
}

func processJoinerSmallTableData[S proto.Message, B proto.Message](
	joinerStage *common.JoinerStageData[S, B],
	dir string,
	marshaler protojson.MarshalOptions,
	timestamp string,
) error {
	// log.Infof("Processing joiner small table data to file: %s", tempDataFilePath)

	jsonMap := make(map[string]json.RawMessage)

	for key, msg := range joinerStage.SmallTable.Data {
		marshaledData, err := marshaler.Marshal(msg)
		if err != nil {
			return fmt.Errorf("error marshaling data to JSON: %w", err)
		}
		jsonMap[key] = marshaledData
	}

	outputData := JoinerOutputFile[map[string]json.RawMessage]{
		Data:                jsonMap,
		Timestamp:           timestamp,
		Status:              VALID_STATUS,
		OmegaProcessed:      joinerStage.SmallTable.OmegaProcessed,
		RingRound:           joinerStage.RingRound,
		Ready:               joinerStage.SmallTable.Ready,
		SmallReadyToDelete:  joinerStage.SmallTable.IsReadyToDelete,
		BigReadyToDelete:    joinerStage.BigTable.IsReadyToDelete,
		SendedTaskCount:     joinerStage.SendedTaskCount,
		SmallTableTaskCount: joinerStage.SmallTableTaskCount,
	}

	tempDataFilePath := filepath.Join(dir, TEMPORARY_DATA_FILE_NAME+JSON_FILE_EXTENSION)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	if err := writeToTempFile(tempDataFilePath, outputData); err != nil {
		return fmt.Errorf("error writing to temp file: %w", err)
	}

	return nil
}

func processJoinerBigTableData[S, B proto.Message](
	joinerStage *common.JoinerStageData[S, B],
	dir string,
	marshaler protojson.MarshalOptions,
	timestamp string,
) error {
	// log.Infof("Processing joiner big table data to file: %s", tempDataFilePath)

	jsonMap := make(map[int]map[string][]json.RawMessage)
	for outerKey, innerMap := range joinerStage.BigTable.Data {
		jsonMap[outerKey] = make(map[string][]json.RawMessage)
		for innerKey, slice := range innerMap {
			for _, msg := range slice {
				marshaledData, err := marshaler.Marshal(msg)
				if err != nil {
					return fmt.Errorf("error marshaling data to JSON: %w", err)
				}
				jsonMap[outerKey][innerKey] = append(jsonMap[outerKey][innerKey], marshaledData)
			}
		}
	}

	outputData := JoinerOutputFile[map[int]map[string][]json.RawMessage]{
		Data:                jsonMap,
		Timestamp:           timestamp,
		Status:              VALID_STATUS,
		OmegaProcessed:      joinerStage.BigTable.OmegaProcessed,
		RingRound:           joinerStage.RingRound,
		Ready:               joinerStage.BigTable.Ready,
		SmallReadyToDelete:  joinerStage.SmallTable.IsReadyToDelete,
		BigReadyToDelete:    joinerStage.BigTable.IsReadyToDelete,
		SendedTaskCount:     joinerStage.SendedTaskCount,
		SmallTableTaskCount: joinerStage.SmallTableTaskCount,
	}

	tempFilePath := filepath.Join(dir, TEMPORARY_DATA_FILE_NAME+JSON_FILE_EXTENSION)
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	if err := writeToTempFile(tempFilePath, outputData); err != nil {
		return fmt.Errorf("error writing to temp file: %w", err)
	}

	return nil
}

func writeToTempFile[T any](tempFilePath string, output T) error {
	jsonBytes, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling JSON array: %w", err)
	}

	tmpFile, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error creating temp file: %w", err)
	}

	if _, err := tmpFile.Write(jsonBytes); err != nil {
		tmpFile.Close()
		return fmt.Errorf("error writing to temp file: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return fmt.Errorf("error syncing temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("error closing temp file: %w", err)
	}

	return nil
}

func appendLineToFile(filePath string, line string) error {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()

	if _, err := f.WriteString(line + "\n"); err != nil {
		return fmt.Errorf("error writing to file: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("error syncing file: %w", err)
	}
	return nil
}

func marshallLineToAppend(fragment model.TaskFragmentIdentifier, timestamp string) (string, error) {
	fragmentBytes, err := json.Marshal(fragment)
	if err != nil {
		return "", fmt.Errorf("error marshaling fragments: %w", err)
	}

	lineContent := fmt.Sprintf("%s %s", timestamp, string(fragmentBytes))
	lenStr := fmt.Sprintf("%d", len(lineContent))
	finalLine := fmt.Sprintf("%s %s", lenStr, lineContent)
	return finalLine, nil
}

func StartCleanupRoutine(dir string, cleanUpTime time.Duration) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := cleanUpOldFiles(dir, cleanUpTime)
			if err != nil {
				log.Errorf("Error during cleanup: %s", err)
			}

		}
	}
}

func cleanUpOldFiles(dir string, cleanUpTime time.Duration) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if time.Since(info.ModTime()) >= cleanUpTime {
			if err := os.Remove(path); err != nil {
				log.Errorf("Error deleting old file %s: %v", path, err)
			} else {
				parentDir := filepath.Dir(path)
				entries, err := os.ReadDir(parentDir)
				if err == nil && len(entries) == 0 {
					if err := os.Remove(parentDir); err != nil {
						log.Errorf("Error deleting empty directory %s: %v", parentDir, err)
					} else {
						log.Debugf("[storage] Deleted empty directory: %s", parentDir)
					}
				}
			}
		}
		return nil
	})
}

func cleanAllTempFiles(rootDir string) error {
	return filepath.WalkDir(rootDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if (filepath.Ext(path) == JSON_FILE_EXTENSION && (filepath.Base(path) == TEMPORARY_DATA_FILE_NAME+JSON_FILE_EXTENSION ||
			filepath.Base(path) == TEMPORARY_METADATA_FILE_NAME+JSON_FILE_EXTENSION)) || (filepath.Ext(path) == LOG_FILE_EXTENSION && filepath.Base(path) == TEMPORARY_LOG_FILE_NAME+LOG_FILE_EXTENSION) {
			if err := os.Remove(path); err != nil {
				log.Warningf("Error deleting temp file %s: %v", path, err)
			}
		}
		return nil
	})
}

func getStageNameFromInterface(stage interface{}) (string, error) {
	switch stage.(type) {
	case *protocol.Task_Alpha:
		return common.ALPHA_STAGE, nil
	case *protocol.Task_Beta:
		return common.BETA_STAGE, nil
	case *protocol.Task_Gamma:
		return common.GAMMA_STAGE, nil
	case *protocol.Task_Delta_1:
		return common.DELTA_STAGE_1, nil
	case *protocol.Task_Delta_2:
		return common.DELTA_STAGE_2, nil
	case *protocol.Task_Delta_3:
		return common.DELTA_STAGE_3, nil
	case *protocol.Task_Epsilon:
		return common.EPSILON_STAGE, nil
	case *protocol.Task_Zeta:
		return common.ZETA_STAGE, nil
	case *protocol.Task_Eta_1:
		return common.ETA_STAGE_1, nil
	case *protocol.Task_Eta_2:
		return common.ETA_STAGE_2, nil
	case *protocol.Task_Eta_3:
		return common.ETA_STAGE_3, nil
	case *protocol.Task_Theta:
		return common.THETA_STAGE, nil
	case *protocol.Task_Iota:
		return common.IOTA_STAGE, nil
	case *protocol.Task_Kappa_1:
		return common.KAPPA_STAGE_1, nil
	case *protocol.Task_Kappa_2:
		return common.KAPPA_STAGE_2, nil
	case *protocol.Task_Kappa_3:
		return common.KAPPA_STAGE_3, nil
	case *protocol.Task_Lambda:
		return common.LAMBDA_STAGE, nil
	case *protocol.Task_Mu:
		return common.MU_STAGE, nil
	case *protocol.Task_Nu_1:
		return common.NU_STAGE_1, nil
	case *protocol.Task_Nu_2:
		return common.NU_STAGE_2, nil
	case *protocol.Task_Nu_3:
		return common.NU_STAGE_3, nil
	case *protocol.Task_RingEOF:
		return common.RING_STAGE, nil
	case *protocol.Task_OmegaEOF:
		return common.OMEGA_STAGE, nil
	default:
		return "", fmt.Errorf("unknown stage type")
	}
}

func appendTaskFragmentIdentifiersLogLiteral(dir string, clientId string, stage string, tableType common.FolderType, timestamp string, fragment model.TaskFragmentIdentifier) error {
	dirPath := filepath.Join(dir, stage, string(tableType), clientId)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	logFilePath := filepath.Join(dirPath, FINAL_LOG_FILE_NAME+LOG_FILE_EXTENSION)
	log.Debugf("[storage] appendTaskFragmentIdentifiersLogLiteral: dir=%s, clientId=%s, stage=%s, tableType=%s, timestamp=%s, fragment=%v",
		dir, clientId, stage, tableType, timestamp, fragment)
	finalLine, err := marshallLineToAppend(fragment, timestamp)
	if err != nil {
		return fmt.Errorf("error preparing line to append to file: %w", err)
	}

	log.Debugf("[storage] finalLine to append: %s", finalLine)
	if err := appendLineToFile(logFilePath, finalLine); err != nil {
		return fmt.Errorf("error appending task fragment identifiers log literal: %w", err)
	}
	log.Debugf("[storage] Successfully appended task fragment identifiers log literal for stage %s, table type %s, client ID %s", stage, tableType, clientId)

	return nil
}

func appendTaskFragmentIdentifiersLogLiteralByPath(
	dirPath string, timestamp string, fragment model.TaskFragmentIdentifier,
) error {
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	logFilePath := filepath.Join(dirPath, FINAL_LOG_FILE_NAME+LOG_FILE_EXTENSION)

	finalLine, err := marshallLineToAppend(fragment, timestamp)
	if err != nil {
		return fmt.Errorf("error preparing line to append to file: %w", err)
	}

	if err := appendLineToFile(logFilePath, finalLine); err != nil {
		return fmt.Errorf("error appending task fragment identifiers log literal: %w", err)
	}

	return nil

}

func readAndFilterTaskFragmentIdentifiersLogLiteral(
	dir string,
	dataTimestamp string, stage string,
) (map[model.TaskFragmentIdentifier]common.FragmentStatus, error) {

	log.Debugf("[Storage] readAndFilterTaskFragmentIdentifiersLogLiteral: dir=%s, dataTimestamp=%s", dir, dataTimestamp)

	if dataTimestamp == "" {
		return nil, fmt.Errorf("data timestamp is empty")
	}

	logFilePath := filepath.Join(dir, FINAL_LOG_FILE_NAME+LOG_FILE_EXTENSION)
	tempLogFilePath := filepath.Join(dir, TEMPORARY_LOG_FILE_NAME+LOG_FILE_EXTENSION)

	file, err := os.Open(logFilePath)
	if err != nil {
		return make(map[model.TaskFragmentIdentifier]common.FragmentStatus), nil
	}
	defer file.Close()

	tempFile, err := os.OpenFile(tempLogFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening temp log file: %w", err)
	}
	defer tempFile.Close()

	cutoffTimestamp, err := time.Parse(time.RFC3339Nano, dataTimestamp)
	if err != nil {
		return nil, fmt.Errorf("invalid cutoff timestamp: %w", err)
	}

	fragments := make(map[model.TaskFragmentIdentifier]common.FragmentStatus)
	scanner := bufio.NewScanner(file)
	stopProcessing := false

	for scanner.Scan() {
		line := scanner.Text()
		if stopProcessing {
			break
		}
		// Extract length and content
		spaceIdx := strings.Index(line, " ")
		if spaceIdx == -1 {
			log.Warningf("Malformed log line: %s", line)
			continue
		}
		lengthStr := line[:spaceIdx]
		content := line[spaceIdx+1:]
		expectedLen, err := strconv.Atoi(lengthStr)
		if err != nil {
			log.Warningf("[storage] Invalid length in log line: %s", line)
			continue
		}
		if len(content) != expectedLen {
			log.Warningf("[storage] Length mismatch: got %d, expected %d, line: %s", len(content), expectedLen, line)
			continue
		}
		// Parse timestamp and content
		spaceIdx2 := strings.Index(content, " ")
		if spaceIdx2 == -1 {
			continue
		}
		timestampStr := content[:spaceIdx2]
		timestamp, err := time.Parse(time.RFC3339Nano, timestampStr)
		if err != nil {
			continue
		}

		if timestamp.After(cutoffTimestamp) {
			log.Debugf("[storage] Skipping log line after cutoff timestamp: %s", timestampStr)
			stopProcessing = true
			continue
		}

		jsonStr := content[spaceIdx2+1:]
		log.Debugf("[storage] Processing log line: %s", jsonStr)
		var frag model.TaskFragmentIdentifier
		if err := json.Unmarshal([]byte(jsonStr), &frag); err != nil {
			panic(fmt.Errorf("error unmarshaling task fragment identifier: %w", err))
			// continue
		}

		log.Debugf("[storage] Found task fragment identifier: %v", frag)

		fragments[frag] = common.FragmentStatus{Logged: true}

		if _, err := tempFile.WriteString(line + "\n"); err != nil {
			return nil, fmt.Errorf("error writing to temp log file: %w", err)
		}
		log.Debugf("[storage] Appended log line: %s", line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading log file: %w", err)
	}
	if err := tempFile.Sync(); err != nil {
		return nil, fmt.Errorf("error syncing temp log file: %w", err)
	}

	if err := os.Rename(tempLogFilePath, logFilePath); err != nil {
		return nil, fmt.Errorf("error renaming temp log file: %w", err)
	}

	log.Debugf("[Storage] Successfully read and filtered task fragment identifiers log literal for dir %s, dataTimestamp %s", dir, dataTimestamp)
	return fragments, nil
}

// Devuelve las claves del mapa de partialResults para logging/debug
func keysOfJoinerPartialResults(m map[string]*common.JoinerPartialResults) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
