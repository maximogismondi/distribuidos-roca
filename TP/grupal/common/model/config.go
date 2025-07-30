package model

import (
	"fmt"
	"path/filepath"
	"time"
)

type WorkerClusterConfig struct {
	FilterCount   int
	OverviewCount int
	MapCount      int
	JoinCount     int
	ReduceCount   int
	MergeCount    int
	TopCount      int
	HealthCount   int
	ServerCount   int // Added for server workers
}

func (w *WorkerClusterConfig) TotalWorkers() int {
	return w.FilterCount +
		w.OverviewCount +
		w.MapCount +
		w.JoinCount +
		w.ReduceCount +
		w.MergeCount +
		w.TopCount
}

type RabbitConfig struct {
	FilterExchange     string
	OverviewExchange   string
	MapExchange        string
	JoinExchange       string
	ReduceExchange     string
	MergeExchange      string
	TopExchange        string
	ResultExchange     string
	EofExchange        string
	BroadcastID        string
	EofBroadcastRK     string
	ParkingEOFExchange string
	ControlExchange    string
	ControlBroadcastRK string
	LeaderRK           string
	HealthExchange     string
	ClientQueueTTL     string
}

type InfraConfig struct {
	nodeID        string
	workers       *WorkerClusterConfig
	rabbit        *RabbitConfig
	volumeBaseDir string
	cleanUpTime   int64 // in seconds
}

func NewInfraConfig(idNode string, workerConfig *WorkerClusterConfig, rabbitConfig *RabbitConfig, volumeBaseDir string, cleanUpTime int64) *InfraConfig {
	return &InfraConfig{
		nodeID:        idNode,
		workers:       workerConfig,
		rabbit:        rabbitConfig,
		volumeBaseDir: volumeBaseDir,
		cleanUpTime:   cleanUpTime,
	}
}

func (i *InfraConfig) GetWorkersCountByType(workerType string) int {
	kind := ActionType(workerType)

	switch kind {
	case FilterAction:
		return i.workers.FilterCount
	case OverviewerAction:
		return i.workers.OverviewCount
	case MapperAction:
		return i.workers.MapCount
	case JoinerAction:
		return i.workers.JoinCount
	case ReducerAction:
		return i.workers.ReduceCount
	case MergerAction:
		return i.workers.MergeCount
	case TopperAction:
		return i.workers.TopCount
	default:
		return 0
	}
}

func (i *InfraConfig) GetNodeId() string {
	return i.nodeID
}

func (i *InfraConfig) GetWorkers() *WorkerClusterConfig {
	return i.workers
}

func (i *InfraConfig) GetRabbit() *RabbitConfig {
	return i.rabbit
}

func (i *InfraConfig) GetFilterCount() int {
	return i.workers.FilterCount
}

func (i *InfraConfig) GetOverviewCount() int {
	return i.workers.OverviewCount
}

func (i *InfraConfig) GetMapCount() int {
	return i.workers.MapCount
}

func (i *InfraConfig) GetJoinCount() int {
	return i.workers.JoinCount
}

func (i *InfraConfig) GetReduceCount() int {
	return i.workers.ReduceCount
}

func (i *InfraConfig) GetMergeCount() int {
	return i.workers.MergeCount
}

func (i *InfraConfig) GetTopCount() int {
	return i.workers.TopCount
}

func (i *InfraConfig) GetTotalWorkers() int {
	return i.workers.TotalWorkers()
}

func (i *InfraConfig) GetFilterExchange() string {
	return i.rabbit.FilterExchange
}

func (i *InfraConfig) GetOverviewExchange() string {
	return i.rabbit.OverviewExchange
}

func (i *InfraConfig) GetMapExchange() string {
	return i.rabbit.MapExchange
}

func (i *InfraConfig) GetJoinExchange() string {
	return i.rabbit.JoinExchange
}

func (i *InfraConfig) GetReduceExchange() string {
	return i.rabbit.ReduceExchange
}

func (i *InfraConfig) GetMergeExchange() string {
	return i.rabbit.MergeExchange
}

func (i *InfraConfig) GetTopExchange() string {
	return i.rabbit.TopExchange
}

func (i *InfraConfig) GetResultExchange() string {
	return i.rabbit.ResultExchange
}

func (i *InfraConfig) GetEofExchange() string {
	return i.rabbit.EofExchange
}

func (i *InfraConfig) GetControlExchange() string {
	return i.rabbit.ControlExchange
}

func (i *InfraConfig) GetHealthExchange() string {
	return i.rabbit.HealthExchange
}

func (i *InfraConfig) GetControlBroadcastRK() string {
	return i.rabbit.ControlBroadcastRK
}

func (i *InfraConfig) GetLeaderRK() string {
	return i.rabbit.LeaderRK
}

func (i *InfraConfig) GetBroadcastID() string {
	return i.rabbit.BroadcastID
}

func (i *InfraConfig) GetEofBroadcastRK() string {
	return i.rabbit.EofBroadcastRK
}

func (i *InfraConfig) GetWorkerDirectory(workerType string, workerID string) string {
	return filepath.Join(i.volumeBaseDir, fmt.Sprintf("%s_%s", workerType, workerID))
}

func (i *InfraConfig) GetDirectory() string {
	return i.volumeBaseDir
}

func (i *InfraConfig) GetParkingEOFExchange() string {
	return i.rabbit.ParkingEOFExchange
}

func (i *InfraConfig) GetClientQueueTTL() string {
	if i.rabbit.ClientQueueTTL == "" {
		return "1800000" // Default to 30 minutes if not set
	}
	return i.rabbit.ClientQueueTTL
}

func (i *InfraConfig) GetCleanUpTime() time.Duration {
	if i.cleanUpTime <= 0 {
		return 60 * time.Second // Default to 60 seconds if not set
	}
	return time.Duration(i.cleanUpTime) * time.Second
}

func (i *InfraConfig) GetHealthCount() int {
	return i.workers.HealthCount
}

func (i *InfraConfig) GetServerCount() int {
	return i.workers.ServerCount
}
