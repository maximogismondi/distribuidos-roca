package storage

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"

	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils/topkheap"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

const CLIENT_ID = "test_client"
const ANOTHER_CLIENT_ID = "another_test_client"
const DIR = "prueba"
const EPSILON_TOP_K = 5
const LAMBDA_TOP_K = 10
const THETA_TOP_K = 1

func LoadDataToFile[T proto.Message](t *testing.T, tc struct {
	name     string
	data     *common.PartialData[T]
	dir      string
	clientID string
	source   string
	stage    interface{}
}) {
	taskFragment := model.TaskFragmentIdentifier{}

	err := SaveDataToFile(tc.dir, tc.clientID, tc.stage, common.FolderType(tc.source), tc.data, taskFragment)
	assert.NoError(t, err, "Failed to save data to file")
	assert.NoError(t, err, "Failed to commit partial data to final")

}

func LoadMetadataToFile[T proto.Message](t *testing.T, tc struct {
	name     string
	data     *common.PartialData[T]
	dir      string
	clientID string
	source   common.FolderType
	stage    string
}) {

	err := SaveMetadataToFile(tc.dir, tc.clientID, tc.stage, tc.source, tc.data)
	assert.NoError(t, err, "Failed to save data to file")
	assert.NoError(t, err, "Failed to commit partial data to final")

}

func loadMergerPartialResultsFromDisk(t *testing.T, tempDir string) map[string]*common.MergerPartialResults {
	actualResult, err := LoadMergerPartialResultsFromDisk(tempDir)
	assert.NoError(t, err, "Failed to load data from file")

	return actualResult
}

// Compare functions
func compareTopperPartialResults(a, b *common.TopperPartialResults) bool {
	if a == nil && b == nil {
		log.Info("Both TopperPartialResults are nil")
		return true
	}
	if a == nil || b == nil {
		log.Error("One of the TopperPartialResults is nil")
		return false
	}

	// EpsilonData
	if (a.EpsilonData == nil) != (b.EpsilonData == nil) {
		log.Error("EpsilonData nil mismatch")
		return false
	}
	if a.EpsilonData != nil {
		if a.EpsilonData.OmegaProcessed != b.EpsilonData.OmegaProcessed {
			log.Errorf("EpsilonData.OmegaProcessed mismatch: %v vs %v", a.EpsilonData.OmegaProcessed, b.EpsilonData.OmegaProcessed)
			return false
		}
		if a.EpsilonData.RingRound != b.EpsilonData.RingRound {
			log.Errorf("EpsilonData.RingRound mismatch: %v vs %v", a.EpsilonData.RingRound, b.EpsilonData.RingRound)
			return false
		}
		// Comparación manual de heaps
		aItems := a.EpsilonData.Heap.GetTopK()
		bItems := b.EpsilonData.Heap.GetTopK()
		if len(aItems) != len(bItems) {
			log.Errorf("Epsilon Heap lengths: %d vs %d", len(aItems), len(bItems))
			return false
		}
		for i := range aItems {
			if !proto.Equal(aItems[i], bItems[i]) {
				log.Errorf("Epsilon Heap mismatch at index %d", i)
				log.Infof("a.EpsilonData.Heap[%d]: %+v", i, aItems[i])
				log.Infof("b.EpsilonData.Heap[%d]: %+v", i, bItems[i])
				return false
			}
		}
	}

	// LamdaData
	if (a.LamdaData == nil) != (b.LamdaData == nil) {
		log.Error("LamdaData nil mismatch")
		return false
	}
	if a.LamdaData != nil {
		if a.LamdaData.OmegaProcessed != b.LamdaData.OmegaProcessed {
			log.Errorf("LamdaData.OmegaProcessed mismatch: %v vs %v", a.LamdaData.OmegaProcessed, b.LamdaData.OmegaProcessed)
			return false
		}
		if a.LamdaData.RingRound != b.LamdaData.RingRound {
			log.Errorf("LamdaData.RingRound mismatch: %v vs %v", a.LamdaData.RingRound, b.LamdaData.RingRound)
			return false
		}
		aItems := a.LamdaData.Heap.GetTopK()
		bItems := b.LamdaData.Heap.GetTopK()
		if len(aItems) != len(bItems) {
			log.Errorf("Lambda Heap lengths: %d vs %d", len(aItems), len(bItems))
			return false
		}
		for i := range aItems {
			if !proto.Equal(aItems[i], bItems[i]) {
				log.Errorf("Lambda Heap mismatch at index %d", i)
				log.Infof("a.LamdaData.Heap[%d]: %+v", i, aItems[i])
				log.Infof("b.LamdaData.Heap[%d]: %+v", i, bItems[i])
				return false
			}
		}
	}

	// ThetaData
	if (a.ThetaData == nil) != (b.ThetaData == nil) {
		log.Error("ThetaData nil mismatch")
		return false
	}
	if a.ThetaData != nil {
		// MinPartialData
		if (a.ThetaData.MinPartialData == nil) != (b.ThetaData.MinPartialData == nil) {
			log.Error("ThetaData.MinPartialData nil mismatch")
			return false
		}
		if a.ThetaData.MinPartialData != nil {
			if a.ThetaData.MinPartialData.OmegaProcessed != b.ThetaData.MinPartialData.OmegaProcessed {
				log.Errorf("ThetaData.MinPartialData.OmegaProcessed mismatch: %v vs %v", a.ThetaData.MinPartialData.OmegaProcessed, b.ThetaData.MinPartialData.OmegaProcessed)
				return false
			}
			if a.ThetaData.MinPartialData.RingRound != b.ThetaData.MinPartialData.RingRound {
				log.Errorf("ThetaData.MinPartialData.RingRound mismatch: %v vs %v", a.ThetaData.MinPartialData.RingRound, b.ThetaData.MinPartialData.RingRound)
				return false
			}
			aItems := a.ThetaData.MinPartialData.Heap.GetTopK()
			bItems := b.ThetaData.MinPartialData.Heap.GetTopK()
			if len(aItems) != len(bItems) {
				log.Errorf("Theta Min Heap lengths: %d vs %d", len(aItems), len(bItems))
				return false
			}
			for i := range aItems {
				if !proto.Equal(aItems[i], bItems[i]) {
					log.Errorf("Theta Min Heap mismatch at index %d", i)
					log.Infof("a.ThetaData.MinPartialData.Heap[%d]: %+v", i, aItems[i])
					log.Infof("b.ThetaData.MinPartialData.Heap[%d]: %+v", i, bItems[i])
					return false
				}
			}
		}
		// MaxPartialData
		if (a.ThetaData.MaxPartialData == nil) != (b.ThetaData.MaxPartialData == nil) {
			log.Error("ThetaData.MaxPartialData nil mismatch")
			return false
		}
		if a.ThetaData.MaxPartialData != nil {
			if a.ThetaData.MaxPartialData.OmegaProcessed != b.ThetaData.MaxPartialData.OmegaProcessed {
				log.Errorf("ThetaData.MaxPartialData.OmegaProcessed mismatch: %v vs %v", a.ThetaData.MaxPartialData.OmegaProcessed, b.ThetaData.MaxPartialData.OmegaProcessed)
				return false
			}
			if a.ThetaData.MaxPartialData.RingRound != b.ThetaData.MaxPartialData.RingRound {
				log.Errorf("ThetaData.MaxPartialData.RingRound mismatch: %v vs %v", a.ThetaData.MaxPartialData.RingRound, b.ThetaData.MaxPartialData.RingRound)
				return false
			}
			aItems := a.ThetaData.MaxPartialData.Heap.GetTopK()
			bItems := b.ThetaData.MaxPartialData.Heap.GetTopK()
			if len(aItems) != len(bItems) {
				log.Errorf("Theta Max Heap lengths: %d vs %d", len(aItems), len(bItems))
				return false
			}
			for i := range aItems {
				if !proto.Equal(aItems[i], bItems[i]) {
					log.Errorf("Theta Max Heap mismatch at index %d", i)
					log.Infof("a.ThetaData.MaxPartialData.Heap[%d]: %+v", i, aItems[i])
					log.Infof("b.ThetaData.MaxPartialData.Heap[%d]: %+v", i, bItems[i])
					return false
				}
			}
		}
	}

	log.Info("TopperPartialResults are equal")
	return true
}
func compareProtobufMaps(expected, actual interface{}) bool {
	expectedVal := reflect.ValueOf(expected)
	actualVal := reflect.ValueOf(actual)

	// Verify that both are maps
	if expectedVal.Kind() != reflect.Map || actualVal.Kind() != reflect.Map {
		log.Error("The provided values are not maps")
		return false
	}

	// Verify that they have the same size
	if expectedVal.Len() != actualVal.Len() {
		log.Error("The maps have different lengths")
		return false
	}

	// Iterate over the keys of the expected map
	for _, key := range expectedVal.MapKeys() {
		expectedValue := expectedVal.MapIndex(key).Interface()
		actualValue := actualVal.MapIndex(key)

		// Verify that the key exists in the actual map
		if !actualValue.IsValid() {
			log.Errorf("Key %v not found in actual map", key)
			return false
		}

		// Verify that the values are equal using proto.Equal
		if !proto.Equal(expectedValue.(proto.Message), actualValue.Interface().(proto.Message)) {
			log.Errorf("Values for key %v do not match: expected %v, got %v", key, expectedValue, actualValue.Interface())
			return false
		}
	}

	return true
}
func CompareMergerPartialResultsMap(
	expected, actual map[string]*common.MergerPartialResults,
) bool {
	if len(expected) != len(actual) {
		log.Errorf("Different number of clients: expected %d, got %d", len(expected), len(actual))
		return false
	}
	for clientID, expectedResult := range expected {
		actualResult, ok := actual[clientID]
		if !ok {
			log.Errorf("Client %s not found in actual results", clientID)
			return false
		}
		// Comparar Delta3
		if !compareStruct(expectedResult.Delta3, actualResult.Delta3) {
			log.Errorf("Delta3 mismatch for client %s", clientID)
			return false
		}

		// Comparar Eta3
		if !compareStruct(expectedResult.Eta3, actualResult.Eta3) {
			log.Errorf("Eta3 mismatch for client %s", clientID)
			return false
		}
		// Comparar Kappa3
		if !compareStruct(expectedResult.Kappa3, actualResult.Kappa3) {
			log.Errorf("Kappa3 mismatch for client %s", clientID)
			return false
		}
		// Comparar Nu3
		if !compareStruct(expectedResult.Nu3, actualResult.Nu3) {
			log.Errorf("Nu3 mismatch for client %s", clientID)
			return false
		}
	}
	return true
}
func CompareReducerPartialResultsMap(
	expected, actual map[string]*common.ReducerPartialResults,
) bool {
	if len(expected) != len(actual) {
		log.Errorf("Different number of clients: expected %d, got %d", len(expected), len(actual))
		return false
	}
	for clientID, expectedResult := range expected {
		actualResult, ok := actual[clientID]
		if !ok {
			log.Errorf("Client %s not found in actual results", clientID)
			return false
		}
		// Comparar Delta2
		if !compareStruct(expectedResult.Delta2, actualResult.Delta2) {
			log.Errorf("Delta2 mismatch for client %s", clientID)
			return false
		}

		// Comparar Eta2
		if !compareStruct(expectedResult.Eta2, actualResult.Eta2) {
			log.Errorf("Eta2 mismatch for client %s", clientID)
			return false
		}
		// Comparar Kappa2
		if !compareStruct(expectedResult.Kappa2, actualResult.Kappa2) {
			log.Errorf("Kappa2 mismatch for client %s", clientID)
			return false
		}
		// Comparar Nu2
		if !compareStruct(expectedResult.Nu2, actualResult.Nu2) {
			log.Errorf("Nu2 mismatch for client %s", clientID)
			return false
		}
	}
	return true
}
func CompareProtobufMapsOfArrays(expected, actual interface{}) bool {
	expectedVal := reflect.ValueOf(expected)
	actualVal := reflect.ValueOf(actual)

	// Verify that both are maps
	if expectedVal.Kind() != reflect.Map || actualVal.Kind() != reflect.Map {
		log.Error("The provided values are not maps")
		return false
	}

	// Verify that they have the same size
	if expectedVal.Len() != actualVal.Len() {
		log.Error("The maps have different lengths")
		return false
	}

	// Iterate over the keys of the expected map
	for _, key := range expectedVal.MapKeys() {
		expectedValue := expectedVal.MapIndex(key).Interface()
		actualValue := actualVal.MapIndex(key)

		// Verify that the key exists in the actual map
		if !actualValue.IsValid() {
			log.Errorf("Key %v not found in actual map", key)
			return false
		}

		// Verify that the values are slices
		expectedSlice := reflect.ValueOf(expectedValue)
		actualSlice := reflect.ValueOf(actualValue.Interface())

		if expectedSlice.Kind() != reflect.Slice || actualSlice.Kind() != reflect.Slice {
			log.Errorf("Values for key %v are not slices", key)
			return false
		}

		// Verify that the slices have the same length
		if expectedSlice.Len() != actualSlice.Len() {
			log.Errorf("Slices for key %v have different lengths", key)
			return false
		}

		// Compare the elements of the slices
		for i := 0; i < expectedSlice.Len(); i++ {
			if !proto.Equal(expectedSlice.Index(i).Interface().(proto.Message), actualSlice.Index(i).Interface().(proto.Message)) {
				log.Errorf("Elements at index %d for key %v do not match: expected %v, got %v", i, key, expectedSlice.Index(i).Interface(), actualSlice.Index(i).Interface())
				return false
			}
		}
	}

	return true

}

func compareTaskFragmentsMap(
	expected, actual map[model.TaskFragmentIdentifier]common.FragmentStatus,
) bool {
	if len(expected) != len(actual) {
		log.Errorf("TaskFragments map length mismatch: expected %d, got %d", len(expected), len(actual))
		return false
	}
	for k, v := range expected {
		actualV, ok := actual[k]
		if !ok {
			log.Errorf("TaskFragments missing key: %+v", k)
			return false
		}
		if !reflect.DeepEqual(v, actualV) {
			log.Errorf("TaskFragments value mismatch for key %+v: expected %+v, got %+v", k, v, actualV)
			return false
		}
	}
	return true
}
func compareStruct(expected, actual interface{}) bool {
	expectedVal := reflect.ValueOf(expected)
	actualVal := reflect.ValueOf(actual)

	// Si son punteros, obtener el valor al que apuntan
	if expectedVal.Kind() == reflect.Ptr {
		if expectedVal.IsNil() && actualVal.IsNil() {
			return true
		}
		if expectedVal.IsNil() || actualVal.IsNil() {
			return false
		}
		expectedVal = expectedVal.Elem()
	}
	if actualVal.Kind() == reflect.Ptr {
		actualVal = actualVal.Elem()
	}

	// Ambos deben ser structs
	if expectedVal.Kind() != reflect.Struct || actualVal.Kind() != reflect.Struct {
		log.Error("Both values must be structs")
		return false
	}

	t := expectedVal.Type()
	for i := 0; i < expectedVal.NumField(); i++ {
		fieldName := t.Field(i).Name
		expectedField := expectedVal.Field(i).Interface()
		actualField := actualVal.Field(i).Interface()

		// EXCEPCIÓN: TaskFragments se compara con DeepEqual
		if fieldName == "TaskFragments" {
			if !compareTaskFragmentsMap(
				expectedField.(map[model.TaskFragmentIdentifier]common.FragmentStatus),
				actualField.(map[model.TaskFragmentIdentifier]common.FragmentStatus),
			) {
				return false
			}
			continue
		}

		// Si el campo es un mapa, usar compareProtobufMaps
		if expectedVal.Field(i).Kind() == reflect.Map {
			if !compareProtobufMaps(expectedField, actualField) {
				log.Errorf("Map field %s does not match", fieldName)
				return false
			}
			// Si el campo es un struct, comparar recursivamente
		} else if expectedVal.Field(i).Kind() == reflect.Struct {
			if !compareStruct(expectedField, actualField) {
				log.Errorf("Struct field %s does not match", fieldName)
				return false
			}
			// Para el resto, usar DeepEqual
		} else {
			log.Infof("Comparing field %s", fieldName)
			log.Infof("Expected: %v, Actual: %v", expectedField, actualField)
			if !reflect.DeepEqual(expectedField, actualField) {
				log.Errorf("Field %s does not match: expected %v, got %v", fieldName, expectedField, actualField)
				return false
			}
		}
	}
	return true
}

func CompareJoinerPartialResults(expected, actual *common.JoinerPartialResults) bool {
	if expected == nil && actual == nil {
		return true
	}
	if expected == nil || actual == nil {
		return false
	}
	return compareJoinerStageData(expected.ZetaData, actual.ZetaData) &&
		compareJoinerStageData(expected.IotaData, actual.IotaData)
}

func compareJoinerStageData[S proto.Message, B proto.Message](
	expected, actual *common.JoinerStageData[S, B],
) bool {
	if expected == nil && actual == nil {
		return true
	}
	if expected == nil || actual == nil {
		return false
	}
	// Compare SmallTable
	if !compareJoinerTableDataSmall(expected.SmallTable, actual.SmallTable) {
		return false
	}
	// Compare BigTable
	if !compareJoinerTableDataBig(expected.BigTable, actual.BigTable) {
		return false
	}
	// Compare other fields
	if expected.SendedTaskCount != actual.SendedTaskCount ||
		expected.SmallTableTaskCount != actual.SmallTableTaskCount ||
		expected.RingRound != actual.RingRound {
		return false
	}
	return true
}

func compareJoinerTableDataSmall[S proto.Message](
	expected, actual *common.JoinerTableData[common.SmallTableData[S]],
) bool {
	if expected == nil && actual == nil {
		return true
	}
	if expected == nil || actual == nil {
		return false
	}
	if expected.Ready != actual.Ready ||
		expected.OmegaProcessed != actual.OmegaProcessed {
		return false
	}
	// Compare Data map
	if len(expected.Data) != len(actual.Data) {
		return false
	}
	for k, v := range expected.Data {
		av, ok := actual.Data[k]
		if !ok || !proto.Equal(v, av) {
			return false
		}
	}
	return true
}

func compareJoinerTableDataBig[B proto.Message](
	expected, actual *common.JoinerTableData[common.BigTableData[B]],
) bool {
	if expected == nil && actual == nil {
		return true
	}
	if expected == nil || actual == nil {
		return false
	}
	if expected.Ready != actual.Ready ||
		expected.OmegaProcessed != actual.OmegaProcessed {
		return false
	}
	// Compare Data map
	if len(expected.Data) != len(actual.Data) {
		return false
	}
	for outerK, innerMap := range expected.Data {
		actualInnerMap, ok := actual.Data[outerK]
		if !ok || len(innerMap) != len(actualInnerMap) {
			return false
		}
		for innerK, expectedSlice := range innerMap {
			actualSlice, ok := actualInnerMap[innerK]
			if !ok || len(expectedSlice) != len(actualSlice) {
				return false
			}
			for i := range expectedSlice {
				if !proto.Equal(expectedSlice[i], actualSlice[i]) {
					return false
				}
			}
		}
	}
	return true
}

func TestSerializationAndDeserializationOfMerger(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "test_serialization")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	defer os.RemoveAll(tempDir)

	mergerPartialResults := &common.MergerPartialResults{
		Delta3: &common.PartialData[*protocol.Delta_3_Data]{
			Data: map[string]*protocol.Delta_3_Data{
				"Country1": {Country: "Country1", PartialBudget: 10000},
				"Country2": {Country: "Country2", PartialBudget: 20000},
				"Country3": {Country: "Country3", PartialBudget: 30000},
			},

			OmegaProcessed: false,
			RingRound:      0,
		},
		Eta3: &common.PartialData[*protocol.Eta_3_Data]{
			Data: map[string]*protocol.Eta_3_Data{
				"MovieId1": {MovieId: "MovieId1", Title: "Title1", Rating: 4.5, Count: 100},

				"MovieId2": {MovieId: "MovieId2", Title: "Title2", Rating: 3.5, Count: 200},
				"MovieId3": {MovieId: "MovieId3", Title: "Title3", Rating: 5.0, Count: 300},
			},
			OmegaProcessed: false,
			RingRound:      0,
		},

		Kappa3: &common.PartialData[*protocol.Kappa_3_Data]{
			Data: map[string]*protocol.Kappa_3_Data{
				"actor1": {ActorId: "actor1", ActorName: "Actor One", PartialParticipations: 8},
				"actor2": {ActorId: "actor2", ActorName: "Actor Two", PartialParticipations: 12},
			},
			OmegaProcessed: false,
			RingRound:      0,
		},
		Nu3: &common.PartialData[*protocol.Nu_3_Data]{
			Data: map[string]*protocol.Nu_3_Data{
				"true":  {Sentiment: true, Ratio: 0.5, Count: 100},
				"false": {Sentiment: false, Ratio: 0.5, Count: 200},
			},
			OmegaProcessed: false,
			RingRound:      0,
		},
	}

	// Delta3
	LoadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Delta_3_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "MergerPartialResults_Delta3",
		data:     mergerPartialResults.Delta3,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Delta_3{},
	})
	LoadMetadataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Delta_3_Data]
		dir      string
		clientID string
		source   common.FolderType
		stage    string
	}{
		name:     "MergerPartialResults_Delta3_Metadata",
		data:     mergerPartialResults.Delta3,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   common.GENERAL_FOLDER_TYPE,
		stage:    common.DELTA_STAGE_3,
	})

	// Eta3
	LoadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Eta_3_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "MergerPartialResults_Eta3",
		data:     mergerPartialResults.Eta3,
		dir:      tempDir,
		clientID: ANOTHER_CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Eta_3{},
	})
	LoadMetadataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Eta_3_Data]
		dir      string
		clientID string
		source   common.FolderType
		stage    string
	}{
		name:     "MergerPartialResults_Eta3_Metadata",
		data:     mergerPartialResults.Eta3,
		dir:      tempDir,
		clientID: ANOTHER_CLIENT_ID,
		source:   common.GENERAL_FOLDER_TYPE,
		stage:    common.ETA_STAGE_3,
	})

	// Kappa3
	LoadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Kappa_3_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "MergerPartialResults_Kappa3",
		data:     mergerPartialResults.Kappa3,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Kappa_3{},
	})
	LoadMetadataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Kappa_3_Data]
		dir      string
		clientID string
		source   common.FolderType
		stage    string
	}{
		name:     "MergerPartialResults_Kappa3_Metadata",
		data:     mergerPartialResults.Kappa3,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   common.GENERAL_FOLDER_TYPE,
		stage:    common.KAPPA_STAGE_3,
	})

	// Nu3
	LoadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Nu_3_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "MergerPartialResults_Nu3",
		data:     mergerPartialResults.Nu3,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Nu_3{},
	})
	LoadMetadataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Nu_3_Data]
		dir      string
		clientID string
		source   common.FolderType
		stage    string
	}{
		name:     "MergerPartialResults_Nu3_Metadata",
		data:     mergerPartialResults.Nu3,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   common.GENERAL_FOLDER_TYPE,
		stage:    common.NU_STAGE_3,
	})

	actualResult := loadMergerPartialResultsFromDisk(t, tempDir)

	expected := make(map[string]*common.MergerPartialResults)
	expected[CLIENT_ID] = &common.MergerPartialResults{
		Delta3: mergerPartialResults.Delta3,
		Kappa3: mergerPartialResults.Kappa3,
		Nu3:    mergerPartialResults.Nu3,
	}
	expected[ANOTHER_CLIENT_ID] = &common.MergerPartialResults{
		Eta3: mergerPartialResults.Eta3,
	}
	assert.True(t, CompareMergerPartialResultsMap(expected, actualResult), "Loaded merger partial results do not match expected")

}

func TestSerializationAndDeserializationOfReducer(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "test_serialization")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	mergerPartialResults := &common.ReducerPartialResults{
		Delta2: &common.PartialData[*protocol.Delta_2_Data]{
			Data: map[string]*protocol.Delta_2_Data{
				"Country1": {Country: "Country1", PartialBudget: 10000},
				"Country2": {Country: "Country2", PartialBudget: 20000},
				"Country3": {Country: "Country3", PartialBudget: 30000},
			},

			OmegaProcessed: false,
			RingRound:      0,
		},
		Eta2: &common.PartialData[*protocol.Eta_2_Data]{
			Data: map[string]*protocol.Eta_2_Data{
				"MovieId1": {MovieId: "MovieId1", Title: "Title1", Rating: 4.5, Count: 100},

				"MovieId2": {MovieId: "MovieId2", Title: "Title2", Rating: 2.5, Count: 200},
				"MovieId3": {MovieId: "MovieId3", Title: "Title3", Rating: 5.0, Count: 300},
			},
			OmegaProcessed: true,
			RingRound:      2,
		},
		Kappa2: &common.PartialData[*protocol.Kappa_2_Data]{
			Data: map[string]*protocol.Kappa_2_Data{
				"actor1": {ActorId: "actor1", ActorName: "Actor One", PartialParticipations: 8},
				"actor2": {ActorId: "actor2", ActorName: "Actor Two", PartialParticipations: 12},
			},
			OmegaProcessed: true,
			RingRound:      0,
		},
		Nu2: &common.PartialData[*protocol.Nu_2_Data]{
			Data: map[string]*protocol.Nu_2_Data{
				"true":  {Sentiment: true, Ratio: 0.5, Count: 100},
				"false": {Sentiment: false, Ratio: 0.5, Count: 200},
			},
			OmegaProcessed: false,
			RingRound:      0,
		},
	}

	// Delta2
	LoadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Delta_2_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "ReducerPartialResults_Delta2",
		data:     mergerPartialResults.Delta2,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Delta_2{},
	})
	LoadMetadataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Delta_2_Data]
		dir      string
		clientID string
		source   common.FolderType
		stage    string
	}{
		name:     "ReducerPartialResults_Delta2_Metadata",
		data:     mergerPartialResults.Delta2,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   common.GENERAL_FOLDER_TYPE,
		stage:    common.DELTA_STAGE_2,
	})

	// Eta2
	LoadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Eta_2_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "ReducerPartialResults_Eta2",
		data:     mergerPartialResults.Eta2,
		dir:      tempDir,
		clientID: ANOTHER_CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Eta_2{},
	})
	LoadMetadataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Eta_2_Data]
		dir      string
		clientID string
		source   common.FolderType
		stage    string
	}{
		name:     "ReducerPartialResults_Eta2_Metadata",
		data:     mergerPartialResults.Eta2,
		dir:      tempDir,
		clientID: ANOTHER_CLIENT_ID,
		source:   common.GENERAL_FOLDER_TYPE,
		stage:    common.ETA_STAGE_2,
	})

	// Kappa2
	LoadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Kappa_2_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "ReducerPartialResults_Kappa2",
		data:     mergerPartialResults.Kappa2,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Kappa_2{},
	})
	LoadMetadataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Kappa_2_Data]
		dir      string
		clientID string
		source   common.FolderType
		stage    string
	}{
		name:     "ReducerPartialResults_Kappa2_Metadata",
		data:     mergerPartialResults.Kappa2,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   common.GENERAL_FOLDER_TYPE,
		stage:    common.KAPPA_STAGE_2,
	})

	// Nu2
	LoadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Nu_2_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "ReducerPartialResults_Nu2",
		data:     mergerPartialResults.Nu2,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Nu_2{},
	})
	LoadMetadataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Nu_2_Data]
		dir      string
		clientID string
		source   common.FolderType
		stage    string
	}{
		name:     "ReducerPartialResults_Nu2_Metadata",
		data:     mergerPartialResults.Nu2,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   common.GENERAL_FOLDER_TYPE,
		stage:    common.NU_STAGE_2,
	})

	actualResult, err := LoadReducerPartialResultsFromDisk(tempDir)
	if err != nil {
		t.Fatalf("Failed to load reducer partial results: %v", err)
	}

	expected := make(map[string]*common.ReducerPartialResults)
	expected[CLIENT_ID] = &common.ReducerPartialResults{
		Delta2: mergerPartialResults.Delta2,
		Kappa2: mergerPartialResults.Kappa2,
		Nu2:    mergerPartialResults.Nu2,
	}
	expected[ANOTHER_CLIENT_ID] = &common.ReducerPartialResults{
		Eta2: mergerPartialResults.Eta2,
	}
	assert.True(t, CompareReducerPartialResultsMap(expected, actualResult), "Loaded reducer partial results do not match expected")

}

func TestSerializationAndDeserializationOfTopperPartialData(t *testing.T) {

	maxHeap := topkheap.NewTopKMaxHeap[float32, *protocol.Theta_Data](1)
	maxHeap.Insert(1200, &protocol.Theta_Data{Id: "id3", Title: "Title 3", AvgRating: 1200})

	partialMax := &common.TopperPartialData[float32, *protocol.Theta_Data]{
		Heap:           maxHeap,
		OmegaProcessed: true,
		RingRound:      2,
	}

	minHeap := topkheap.NewTopKMinHeap[float32, *protocol.Theta_Data](1)
	minHeap.Insert(800, &protocol.Theta_Data{Id: "id2", Title: "Title 2", AvgRating: 800})

	partialMin := &common.TopperPartialData[float32, *protocol.Theta_Data]{
		Heap:           minHeap,
		OmegaProcessed: true,
		RingRound:      2,
	}

	partials := []*common.TopperPartialData[float32, *protocol.Theta_Data]{
		partialMax,
		partialMin,
	}

	thetaPartial := &common.ThetaPartialData{
		MinPartialData: partialMin,
		MaxPartialData: partialMax,
	}

	taskFragment := model.TaskFragmentIdentifier{
		CreatorId:          CLIENT_ID,
		TaskNumber:         1,
		TaskFragmentNumber: 1,
		LastFragment:       true,
	}

	tempDir, err := os.MkdirTemp("", "test_serialization")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	maxValueFunc := func(input *protocol.Theta_Data) float32 {
		return input.GetAvgRating()
	}

	err = SaveTopperThetaDataToFile(tempDir, &protocol.Task_Theta{}, CLIENT_ID, partials, taskFragment, maxValueFunc)
	assert.NoError(t, err, "Failed to save TopperPartialData to file")

	err = SaveTopperMetadataToFile(tempDir, CLIENT_ID, common.THETA_STAGE, partials[0])
	assert.NoError(t, err, "Failed to save TopperMetadata to file")

	// ----------- Epsilon stage (heap de máxima) -----------
	epsilonMaxHeap := topkheap.NewTopKMaxHeap[uint64, *protocol.Epsilon_Data](5)
	epsilonMaxHeap.Insert(500, &protocol.Epsilon_Data{ProdCountry: "country1", TotalInvestment: 500})
	epsilonMaxHeap.Insert(400, &protocol.Epsilon_Data{ProdCountry: "country2", TotalInvestment: 400})
	epsilonPartial := &common.TopperPartialData[uint64, *protocol.Epsilon_Data]{
		Heap:           epsilonMaxHeap,
		OmegaProcessed: true,
		RingRound:      1,
	}
	epsilonMaxValueFunc := func(input *protocol.Epsilon_Data) uint64 {
		return input.GetTotalInvestment()
	}
	err = SaveTopperDataToFile(tempDir, &protocol.Task_Epsilon{}, CLIENT_ID, epsilonPartial, taskFragment, epsilonMaxValueFunc)
	assert.NoError(t, err, "Failed to save TopperPartialData (Epsilon) to file")
	err = SaveTopperMetadataToFile(tempDir, CLIENT_ID, common.EPSILON_STAGE, epsilonPartial)
	assert.NoError(t, err, "Failed to save TopperMetadata (Epsilon) to file")

	// ----------- Lambda stage (heap de máxima) -----------
	lambdaMaxHeap := topkheap.NewTopKMaxHeap[uint64, *protocol.Lambda_Data](10)
	lambdaMaxHeap.Insert(20, &protocol.Lambda_Data{ActorId: "actor1", ActorName: "Actor One", Participations: 20})
	lambdaMaxHeap.Insert(30, &protocol.Lambda_Data{ActorId: "actor2", ActorName: "Actor Two", Participations: 30})
	lambdaPartial := &common.TopperPartialData[uint64, *protocol.Lambda_Data]{
		Heap:           lambdaMaxHeap,
		OmegaProcessed: false,
		RingRound:      0,
	}
	lambdaMaxValueFunc := func(input *protocol.Lambda_Data) uint64 {
		return input.GetParticipations()
	}
	err = SaveTopperDataToFile(tempDir, &protocol.Task_Lambda{}, CLIENT_ID, lambdaPartial, taskFragment, lambdaMaxValueFunc)
	assert.NoError(t, err, "Failed to save TopperPartialData (Lambda) to file")
	err = SaveTopperMetadataToFile(tempDir, CLIENT_ID, common.LAMBDA_STAGE, lambdaPartial)
	assert.NoError(t, err, "Failed to save TopperMetadata (Lambda) to file")

	// Load the saved TopperPartialResults from disk
	loaded, err := LoadTopperPartialResultsFromDisk(tempDir)
	assert.NoError(t, err, "Failed to load TopperPartialData (Lambda) from file")

	expected := common.TopperPartialResults{
		ThetaData:   thetaPartial,
		EpsilonData: epsilonPartial,
		LamdaData:   lambdaPartial,
	}

	log.Infof("Loaded Epsilon Heap: %+v", loaded[CLIENT_ID].EpsilonData.Heap.GetTopK())
	log.Infof("Expected Epsilon Heap: %+v", expected.EpsilonData.Heap.GetTopK())

	log.Infof("Loaded Lambda Heap: %+v", loaded[CLIENT_ID].LamdaData.Heap.GetTopK())
	log.Infof("Expected Lambda Heap: %+v", expected.LamdaData.Heap.GetTopK())

	// Assert the loaded data matches the expected data
	assert.True(t, compareTopperPartialResults(&expected, loaded[CLIENT_ID]), "Loaded TopperPartialResults do not match expected")
}

func TestSerializationAndDeserializationOfJoinerPartialResults(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test_serialization")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// --- ZetaData ---
	zetaSmall := &common.JoinerTableData[common.SmallTableData[*protocol.Zeta_Data_Movie]]{
		Data: common.SmallTableData[*protocol.Zeta_Data_Movie]{
			"movie1": {MovieId: "movie1", Title: "Movie One"},
			"movie2": {MovieId: "movie2", Title: "Movie Two"},
		},
		Ready:          true,
		OmegaProcessed: true,
	}
	zetaBig := &common.JoinerTableData[common.BigTableData[*protocol.Zeta_Data_Rating]]{
		Data: common.BigTableData[*protocol.Zeta_Data_Rating]{
			1: {
				"movie1": {
					{MovieId: "movieId1", Rating: 4.5},
					{MovieId: "movieId1", Rating: 3.5},
				},
			},
		},
		TaskFragments:  map[model.TaskFragmentIdentifier]common.FragmentStatus{},
		Ready:          true,
		OmegaProcessed: false,
	}
	zetaStage := &common.JoinerStageData[*protocol.Zeta_Data_Movie, *protocol.Zeta_Data_Rating]{
		SmallTable:      zetaSmall,
		BigTable:        zetaBig,
		SendedTaskCount: 2,
		RingRound:       1,
	}

	// --- IotaData ---
	iotaSmall := &common.JoinerTableData[common.SmallTableData[*protocol.Iota_Data_Movie]]{
		Data: common.SmallTableData[*protocol.Iota_Data_Movie]{
			"movieId1": {MovieId: "movieId1"},
		},
		TaskFragments:  map[model.TaskFragmentIdentifier]common.FragmentStatus{},
		Ready:          false,
		OmegaProcessed: false,
	}
	iotaBig := &common.JoinerTableData[common.BigTableData[*protocol.Iota_Data_Actor]]{
		Data: common.BigTableData[*protocol.Iota_Data_Actor]{
			2: {
				"movieId1": {
					{MovieId: "movieId1", ActorId: "actorId1", ActorName: "Actor One"},
					{MovieId: "movieId1", ActorId: "actorId2", ActorName: "Actor Two"},
				},
				"movieId2": {
					{MovieId: "movieId2", ActorId: "actorId1", ActorName: "Actor One"},
					{MovieId: "movieId2", ActorId: "actorId2", ActorName: "Actor Two"},
				},
			},
		},
		TaskFragments:  map[model.TaskFragmentIdentifier]common.FragmentStatus{},
		Ready:          true,
		OmegaProcessed: true,
	}
	iotaStage := &common.JoinerStageData[*protocol.Iota_Data_Movie, *protocol.Iota_Data_Actor]{
		SmallTable:          iotaSmall,
		BigTable:            iotaBig,
		SendedTaskCount:     1,
		SmallTableTaskCount: 1,
		RingRound:           2,
	}

	joinerResults := &common.JoinerPartialResults{
		ZetaData: zetaStage,
		IotaData: iotaStage,
	}

	taskFragment := model.TaskFragmentIdentifier{
		CreatorId:          CLIENT_ID,
		TaskNumber:         1,
		TaskFragmentNumber: 1,
		LastFragment:       true,
	}

	// Guardar Zeta
	err = SaveJoinerTableToFile(tempDir, CLIENT_ID, &protocol.Task_Zeta{}, common.JOINER_SMALL_FOLDER_TYPE, zetaSmall, taskFragment)
	assert.NoError(t, err, "Failed to save Zeta small table")

	err = SaveJoinerMetadataToFile(tempDir, CLIENT_ID, common.ZETA_STAGE, common.JOINER_SMALL_FOLDER_TYPE, zetaStage)
	assert.NoError(t, err, "Failed to save Zeta metadata")

	err = SaveJoinerTableToFile(tempDir, CLIENT_ID, &protocol.Task_Zeta{}, common.JOINER_BIG_FOLDER_TYPE, zetaBig, taskFragment)
	assert.NoError(t, err, "Failed to save Zeta big table")

	err = SaveJoinerMetadataToFile(tempDir, CLIENT_ID, common.ZETA_STAGE, common.JOINER_BIG_FOLDER_TYPE, zetaStage)
	assert.NoError(t, err, "Failed to save Zeta metadata")

	// Guardar Iota
	err = SaveJoinerTableToFile(tempDir, CLIENT_ID, &protocol.Task_Iota{}, common.JOINER_SMALL_FOLDER_TYPE, iotaSmall, taskFragment)
	assert.NoError(t, err, "Failed to save Iota small table")

	err = SaveJoinerMetadataToFile(tempDir, CLIENT_ID, common.IOTA_STAGE, common.JOINER_SMALL_FOLDER_TYPE, iotaStage)
	assert.NoError(t, err, "Failed to save Iota metadata")

	err = SaveJoinerTableToFile(tempDir, CLIENT_ID, &protocol.Task_Iota{}, common.JOINER_BIG_FOLDER_TYPE, iotaBig, taskFragment)
	assert.NoError(t, err, "Failed to save Iota big table")

	err = SaveJoinerMetadataToFile(tempDir, CLIENT_ID, common.IOTA_STAGE, common.JOINER_BIG_FOLDER_TYPE, iotaStage)
	assert.NoError(t, err, "Failed to save Iota metadata")

	// Cargar
	loaded, err := LoadJoinerPartialResultsFromDisk(tempDir)
	assert.NoError(t, err, "Failed to load joiner partial results from file")

	// log.Debugf("Loaded task fragments: %v", loaded[CLIENT_ID].ZetaData.SmallTable.TaskFragments)
	// Comparar
	assert.NotNil(t, loaded[CLIENT_ID], "Loaded joiner partial results is nil")
	assert.True(t, CompareJoinerPartialResults(joinerResults, loaded[CLIENT_ID]), "Loaded joiner partial results do not match expected")
}

func TestAppendAndReadTaskFragmentIdentifiersLogLiteral(t *testing.T) {
	dir := t.TempDir()
	clientId := "client1"
	stage := "stage1"
	tableType := "tableType1"

	fragment1 := model.TaskFragmentIdentifier{CreatorId: "c1", TaskNumber: 1, TaskFragmentNumber: 1, LastFragment: false}
	fragment2 := model.TaskFragmentIdentifier{CreatorId: "c2", TaskNumber: 2, TaskFragmentNumber: 2, LastFragment: true}
	timestamp1 := time.Date(2025, 10, 1, 12, 0, 0, 0, time.UTC).Format(time.RFC3339)
	timestamp2 := time.Date(2025, 10, 1, 13, 0, 0, 0, time.UTC).Format(time.RFC3339)

	err := appendTaskFragmentIdentifiersLogLiteral(dir, clientId, stage, common.FolderType(tableType), timestamp1, fragment1)
	if err != nil {
		t.Fatalf("appendTaskFragmentIdentifiersLogLiteral failed: %v", err)
	}
	err = appendTaskFragmentIdentifiersLogLiteral(dir, clientId, stage, common.FolderType(tableType), timestamp2, fragment2)
	if err != nil {
		t.Fatalf("appendTaskFragmentIdentifiersLogLiteral failed: %v", err)
	}

	dirPath := filepath.Join(dir, stage, tableType, clientId)

	frags, err := readAndFilterTaskFragmentIdentifiersLogLiteral(dirPath, timestamp2)
	assert.NoError(t, err, "readAndFilterTaskFragmentIdentifiersLogLiteral failed")
	assert.Len(t, frags, 2, "expected 2 fragments, got %d", len(frags))
	assert.Contains(t, frags, fragment1, "fragment1 not found in results")
	assert.Contains(t, frags, fragment2, "fragment2 not found in results")

	frags2, err := readAndFilterTaskFragmentIdentifiersLogLiteral(dirPath, timestamp1)
	assert.NoError(t, err, "readAndFilterTaskFragmentIdentifiersLogLiteral failed")
	assert.Len(t, frags2, 1, "expected 1 fragment, got %d", len(frags2))
	assert.Contains(t, frags2, fragment1, "fragment1 not found in results")

}

func TestTaskFragmentsPersistenceAndLoad_SingleStageInMerger(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test_taskfragments")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fragment1 := model.TaskFragmentIdentifier{CreatorId: CLIENT_ID, TaskNumber: 1, TaskFragmentNumber: 1, LastFragment: true}
	fragment2 := model.TaskFragmentIdentifier{CreatorId: ANOTHER_CLIENT_ID, TaskNumber: 2, TaskFragmentNumber: 2, LastFragment: true}

	delta3ToLoad := &common.PartialData[*protocol.Delta_3_Data]{
		Data:           map[string]*protocol.Delta_3_Data{"Country1": {Country: "Country1", PartialBudget: 10000}},
		OmegaProcessed: false,
		RingRound:      0,
		TaskFragments: map[model.TaskFragmentIdentifier]common.FragmentStatus{
			fragment1: {Logged: false},
			fragment2: {Logged: false},
		},
	}

	// Guardar fragment1
	err = SaveDataToFile(tempDir, CLIENT_ID, &protocol.Task_Delta_3{}, common.GENERAL_FOLDER_TYPE, delta3ToLoad, fragment1)
	assert.NoError(t, err, "Failed to save fragment1")
	// Guardar fragment2
	err = SaveDataToFile(tempDir, CLIENT_ID, &protocol.Task_Delta_3{}, common.GENERAL_FOLDER_TYPE, delta3ToLoad, fragment2)
	assert.NoError(t, err, "Failed to save fragment2")

	// Guardar metadata
	LoadMetadataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Delta_3_Data]
		dir      string
		clientID string
		source   common.FolderType
		stage    string
	}{
		name:     "Delta3_TaskFragments_Metadata",
		data:     delta3ToLoad,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   common.GENERAL_FOLDER_TYPE,
		stage:    common.DELTA_STAGE_3,
	})

	delta3Expected := &common.PartialData[*protocol.Delta_3_Data]{
		Data:           map[string]*protocol.Delta_3_Data{"Country1": {Country: "Country1", PartialBudget: 10000}},
		OmegaProcessed: false,
		RingRound:      0,
		TaskFragments: map[model.TaskFragmentIdentifier]common.FragmentStatus{
			fragment1: {Logged: true},
			fragment2: {Logged: true},
		},
	}

	mergerExpected := &common.MergerPartialResults{
		Delta3: delta3Expected,
		Eta3:   nil,
		Kappa3: nil,
		Nu3:    nil,
	}

	actual := loadMergerPartialResultsFromDisk(t, tempDir)
	log.Infof("LOADED TASK IDENTIFIERS: %v", actual["test_client"].Delta3.TaskFragments)
	assert.NotNil(t, actual[CLIENT_ID], "Loaded result is nil")
	assert.NotNil(t, actual[CLIENT_ID].Delta3, "Loaded Delta3 is nil")
	assert.Equal(t, delta3ToLoad.Data, actual[CLIENT_ID].Delta3.Data, "Delta3 data mismatch")

	// Verificar que ambos fragmentos están presentes y marcados como Logged
	frags := actual[CLIENT_ID].Delta3.TaskFragments
	assert.Len(t, frags, 2, "Expected 2 task fragments")
	assert.True(t, frags[fragment1].Logged, "fragment1 should be logged")
	assert.True(t, frags[fragment2].Logged, "fragment2 should be logged")

	// El resto debe ser nil
	assert.Nil(t, actual[CLIENT_ID].Eta3, "Eta3 should be nil")
	assert.Nil(t, actual[CLIENT_ID].Kappa3, "Kappa3 should be nil")
	assert.Nil(t, actual[CLIENT_ID].Nu3, "Nu3 should be nil")

	assert.True(t, CompareMergerPartialResultsMap(map[string]*common.MergerPartialResults{CLIENT_ID: mergerExpected}, actual), "Loaded merger partial results do not match expected")
}
