package client_handler

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/model"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/utils"
)

func processMoviesBatch(batch *protocol.Batch) []*model.Movie {
	var ret []*model.Movie

	for _, row := range batch.Data {
		fields := utils.ParseLine(&row.Data)
		movies := utils.ParseMovie(fields)

		ret = append(ret, movies...)
	}

	return ret
}

func processCreditsBatch(batch *protocol.Batch) []*model.Actor {
	var ret []*model.Actor

	for _, row := range batch.Data {
		fields := utils.ParseLine(&row.Data)
		actors := utils.ParseCredit(fields)

		ret = append(ret, actors...)
	}

	return ret
}

func processRatingsBatch(batch *protocol.Batch) []*model.Rating {
	var ret []*model.Rating

	for _, row := range batch.Data {
		fields := utils.ParseLine(&row.Data)
		ratings := utils.ParseRating(fields)

		ret = append(ret, ratings...)
	}

	return ret
}
