package utils

import (
	"github.com/MaxiOtero6/TP-Distribuidos/client/src/client-library/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	m "github.com/MaxiOtero6/TP-Distribuidos/common/model"
)

var QUERIES_AMOUNT int = 5

// ResultParser is a struct that handles the parsing of results from the server
// and stores them in a Results struct.
// It also keeps track of the number of EOF messages received.
type ResultParser struct {
	results model.Results
}

// NewResultParser creates a new ResultParser instance
// and initializes the results field with empty slices/maps for each query type.
func NewResultParser() *ResultParser {
	return &ResultParser{
		results: model.Results{
			Query1: model.NewQueryResults(make([]*model.Query1, 0)),
			Query2: model.NewQueryResults(make([]*model.Query2, 0)),
			Query3: model.NewQueryResults(make(map[string]*model.Query3, 0)),
			Query4: model.NewQueryResults(make([]*model.Query4, 0)),
			Query5: model.NewQueryResults(make(map[string]*model.Query5, 0)),
		},
	}
}

// IsDone checks if the ResultParser has received the expected number of EOF messages.
// If it has, it means that all results have been received and processed.
func (p *ResultParser) IsDone() bool {
	return p.results.Query1.IsComplete() &&
		p.results.Query2.IsComplete() &&
		p.results.Query3.IsComplete() &&
		p.results.Query4.IsComplete() &&
		p.results.Query5.IsComplete()
}

// GetResults returns the parsed results at the moment.
func (p *ResultParser) GetResults() *model.Results {
	return &p.results
}

// handleResult1 processes the Result1 message from the server.
// It extracts the movie ID, title, and genres from the message
// and appends them to the Query1 slice in the results struct.
func (p *ResultParser) handleResult1(data []*protocol.Result1_Data, taskIdentifier *protocol.TaskIdentifier) {
	processResult := func() {
		for _, data := range data {
			query1 := &model.Query1{
				MovieId: data.GetId(),
				Title:   data.GetTitle(),
				Genres:  data.GetGenres(),
			}

			p.results.Query1.Results = append(p.results.Query1.Results, query1)
		}

	}

	p.results.Query1.ProcessResult(processResult, taskIdentifier)
}

// handleResult2 processes the Result2 message from the server.
// It extracts the country and total investment from the message
// and appends them to the Query2 slice in the results struct.
// It also checks if the number of countries returned is within the expected range (up to 5).
// If the number of countries exceeds 5, an error is logged.
// If the Query2 slice already contains data, an error is logged.
func (p *ResultParser) handleResult2(data []*protocol.Result2_Data, taskIdentifier *protocol.TaskIdentifier) {
	if len(data) > 5 {
		log.Errorf("action: handleResult2 | result: fail | error: expected up to 5 elements, got %d", len(data))
		return
	}

	processResult := func() {
		for _, data := range data {
			query2 := &model.Query2{
				Country:         data.GetCountry(),
				TotalInvestment: data.GetTotalInvestment(),
			}

			p.results.Query2.Results = append(p.results.Query2.Results, query2)
		}
	}

	p.results.Query2.ProcessResult(processResult, taskIdentifier)
}

// handleResult3 processes the Result3 message from the server.
// It extracts the maximum and minimum average ratings and titles from the message
// and stores them in the Query3 map in the results struct.
// It checks if the maximum and minimum ratings already exist in the map.
// If they do, an error is logged.
// It also checks if the number of elements in the data slice is exactly 2.
// If not, an error is logged.
// The maximum and minimum ratings are stored in the map with keys "max" and "min".
func (p *ResultParser) handleResult3(data []*protocol.Result3_Data, taskIdentifier *protocol.TaskIdentifier) {
	if len(data) != 2 {
		log.Errorf("action: handleResult3 | result: fail | error: expected 2 elements, got %d", len(data))
		return
	}

	processResult := func() {
		max := &model.Query3{
			AvgRating: data[0].GetRating(),
			Title:     data[0].GetTitle(),
		}

		min := &model.Query3{
			AvgRating: data[1].GetRating(),
			Title:     data[1].GetTitle(),
		}

		p.results.Query3.Results["max"] = max
		p.results.Query3.Results["min"] = min
	}

	p.results.Query3.ProcessResult(processResult, taskIdentifier)
}

// handleResult4 processes the Result4 message from the server.
// It extracts the actor ID and name from the message
// and appends them to the Query4 slice in the results struct.
// It checks if the number of actors returned is within the expected range (up to 10).
// If the number of actors exceeds 10, an error is logged.
// If the Query4 slice already contains data, an error is logged.
// The actors are stored in the Query4 slice.
func (p *ResultParser) handleResult4(data []*protocol.Result4_Data, taskIdentifier *protocol.TaskIdentifier) {
	// Less actors can be returned, but not more than 10
	// This is because the server sends a batch of 10 actors, but the client can
	// receive less than 10 actors if the server has less than 10 actors
	if len(data) > 10 {
		log.Errorf("action: handleResult4 | result: fail | error: expected up to 10 elements, got %d", len(data))
		return
	}

	processResult := func() {
		for _, data := range data {
			query4 := &model.Query4{
				ActorId:        data.GetActorId(),
				ActorName:      data.GetActorName(),
				Participations: data.GetParticipations(),
			}

			p.results.Query4.Results = append(p.results.Query4.Results, query4)
		}
	}

	p.results.Query4.ProcessResult(processResult, taskIdentifier)
}

// handleResult5 processes the Result5 message from the server.
// It extracts the revenue budget ratio from the message
// and stores it in the Query5 map in the results struct.
// It checks if the negative and positive keys already exist in the map.
// If they do, an error is logged.
func (p *ResultParser) handleResult5(data []*protocol.Result5_Data, taskIdentifier *protocol.TaskIdentifier) {
	processResult := func() {
		for _, d := range data {
			var key string
			if d.GetSentiment() {
				key = "POSITIVE"
			} else {
				key = "NEGATIVE"
			}

			p.results.Query5.Results[key] = &model.Query5{
				RevenueBudgetRatio: d.GetRatio(),
			}
		}
	}

	p.results.Query5.ProcessResult(processResult, taskIdentifier)
}

func (p *ResultParser) handleOmegaEOF(omegaEOF *protocol.OmegaEOF_Data) {
	query := omegaEOF.GetStage()

	switch query {
	case m.RESULT_1_STAGE:
		p.results.Query1.ProcessOmega(omegaEOF)
	case m.RESULT_2_STAGE:
		p.results.Query2.ProcessOmega(omegaEOF)
	case m.RESULT_3_STAGE:
		p.results.Query3.ProcessOmega(omegaEOF)
	case m.RESULT_4_STAGE:
		p.results.Query4.ProcessOmega(omegaEOF)
	case m.RESULT_5_STAGE:
		p.results.Query5.ProcessOmega(omegaEOF)
	default:
		log.Errorf("action: handleOmegaEOF | result: fail | error: unknown query stage: %s", query)
		return
	}
}

func (p *ResultParser) Save(results *protocol.ResultsResponse) {
	for _, result := range results.GetResults() {
		taskIdentifier := result.GetTaskIdentifier()

		switch result.Message.(type) {
		case *protocol.ResultsResponse_Result_Result1:
			log.Debugf("action: Save | result: success | Result1 received")
			p.handleResult1(result.GetResult1().GetData(), taskIdentifier)
		case *protocol.ResultsResponse_Result_Result2:
			log.Debugf("action: Save | result: success | Result2 received")
			p.handleResult2(result.GetResult2().GetData(), taskIdentifier)
		case *protocol.ResultsResponse_Result_Result3:
			log.Debugf("action: Save | result: success | Result3 received")
			p.handleResult3(result.GetResult3().GetData(), taskIdentifier)
		case *protocol.ResultsResponse_Result_Result4:
			log.Debugf("action: Save | result: success | Result4 received")
			p.handleResult4(result.GetResult4().GetData(), taskIdentifier)
		case *protocol.ResultsResponse_Result_Result5:
			log.Debugf("action: Save | result: success | Result5 received")
			p.handleResult5(result.GetResult5().GetData(), taskIdentifier)
		case *protocol.ResultsResponse_Result_OmegaEOF:
			data := result.GetOmegaEOF().GetData()
			log.Debugf("action: Save | result: success | EOF received | Query: %s | TaskCount: %d", data.GetStage(), data.GetTasksCount())
			p.handleOmegaEOF(data)
		}
	}
}
