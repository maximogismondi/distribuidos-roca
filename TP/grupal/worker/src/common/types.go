package common

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils/topkheap"
	"google.golang.org/protobuf/proto"
)

// map[Exchange][RoutingKey][]*protocol.Task
type Tasks map[string]map[string][]*protocol.Task

type NextStageData struct {
	Stage       string
	Exchange    string
	WorkerCount int
	RoutingKey  string
}

type FragmentStatus struct {
	Logged bool
}

type PartialData[T proto.Message] struct {
	Data           map[string]T
	TaskFragments  map[model.TaskFragmentIdentifier]FragmentStatus
	OmegaProcessed bool
	RingRound      uint32
	IsReady        bool // Indicates if the data should be deleted in the next stage
}

type MergerPartialResults struct {
	// toDeleteCount uint
	Delta3 *PartialData[*protocol.Delta_3_Data]
	Eta3   *PartialData[*protocol.Eta_3_Data]
	Kappa3 *PartialData[*protocol.Kappa_3_Data]
	Nu3    *PartialData[*protocol.Nu_3_Data]
}

type ReducerPartialResults struct {
	ToDeleteCount uint
	Delta2        *PartialData[*protocol.Delta_2_Data]
	Eta2          *PartialData[*protocol.Eta_2_Data]
	Kappa2        *PartialData[*protocol.Kappa_2_Data]
	Nu2           *PartialData[*protocol.Nu_2_Data]
}

type TopperPartialResults struct {
	EpsilonData *TopperPartialData[uint64, *protocol.Epsilon_Data]
	ThetaData   *ThetaPartialData
	LamdaData   *TopperPartialData[uint64, *protocol.Lambda_Data]
}

type TopperPartialData[K topkheap.Ordered, V any] struct {
	Heap           topkheap.TopKHeap[K, V]
	TaskFragments  map[model.TaskFragmentIdentifier]FragmentStatus
	OmegaProcessed bool
	RingRound      uint32
	IsReady        bool // Indicates if the data should be deleted in the next stage
}

type ThetaPartialData struct {
	MinPartialData *TopperPartialData[float32, *protocol.Theta_Data]
	MaxPartialData *TopperPartialData[float32, *protocol.Theta_Data]
}

type SmallTableData[S proto.Message] map[string]S
type BigTableData[B proto.Message] map[int]map[string][]B

type JoinerTableData[T any] struct {
	Data            T
	TaskFragments   map[model.TaskFragmentIdentifier]FragmentStatus
	Ready           bool // Indicates if the table is ready to be processed
	IsReadyToDelete bool // Indicates if the table should be deleted in the next stage
	OmegaProcessed  bool
}

type JoinerStageData[S, B proto.Message] struct {
	SmallTable          *JoinerTableData[SmallTableData[S]]
	BigTable            *JoinerTableData[BigTableData[B]]
	SendedTaskCount     int
	SmallTableTaskCount int
	RingRound           uint32
}

type JoinerPartialResults struct {
	ZetaData *JoinerStageData[*protocol.Zeta_Data_Movie, *protocol.Zeta_Data_Rating]
	IotaData *JoinerStageData[*protocol.Iota_Data_Movie, *protocol.Iota_Data_Actor]
}
