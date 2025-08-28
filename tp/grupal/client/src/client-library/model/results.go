package model

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
)

type Query1 struct {
	MovieId string   `json:"-"`
	Title   string   `json:"title"`
	Genres  []string `json:"genres"`
}

type Query2 struct {
	Country         string
	TotalInvestment uint64
}

type Query3 struct {
	AvgRating float32
	Title     string
}

type Query4 struct {
	ActorId        string `json:"-"`
	ActorName      string `json:"name"`
	Participations uint64 `json:"count"`
}

type Query5 struct {
	RevenueBudgetRatio float32
}

type QueryResults[T any] struct {
	Results         T
	TaskIdentifiers map[model.TaskFragmentIdentifier]struct{}
	omegaProcessed  bool
	TaskCount       int
}

func NewQueryResults[T any](initialResults T) QueryResults[T] {
	return QueryResults[T]{
		Results:         initialResults,
		TaskCount:       0,
		TaskIdentifiers: make(map[model.TaskFragmentIdentifier]struct{}),
	}
}

func (qr *QueryResults[T]) IsComplete() bool {
	return qr.omegaProcessed && qr.TaskCount == len(qr.TaskIdentifiers)
}

func (qr *QueryResults[T]) ProcessResult(processResult func(), taskIdentifier *protocol.TaskIdentifier) {
	taskID := model.TaskFragmentIdentifier{
		CreatorId:          taskIdentifier.GetCreatorId(),
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	if _, processed := qr.TaskIdentifiers[taskID]; processed {
		return
	}

	qr.TaskIdentifiers[taskID] = struct{}{}
	processResult()
}

func (qr *QueryResults[T]) ProcessOmega(omegaData *protocol.OmegaEOF_Data) {
	qr.omegaProcessed = true

	qr.TaskCount = int(omegaData.GetTasksCount())
}

type Results struct {
	Query1 QueryResults[[]*Query1]
	Query2 QueryResults[[]*Query2]
	Query3 QueryResults[map[string]*Query3]
	Query4 QueryResults[[]*Query4]
	Query5 QueryResults[map[string]*Query5]
}
