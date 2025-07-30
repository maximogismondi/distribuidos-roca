package utils

import (
	"testing"

	"github.com/MaxiOtero6/TP-Distribuidos/server/src/model"
	"github.com/stretchr/testify/assert"
)

func TestGetAlphaStageTask(t *testing.T) {
	CLIENT_ID := "test-client-id"
	movies := []*model.Movie{
		{Id: "0", Title: "Movie 1", ProdCountries: []string{"USA"}, Genres: []string{"Action"}, ReleaseYear: 2020},
		{Id: "1", Title: "Movie 2", ProdCountries: []string{"UK"}, Genres: []string{"Drama"}, ReleaseYear: 2019},
	}

	tasks := GetAlphaStageTask(movies, 2, CLIENT_ID, 0)

	assert.Len(t, tasks, 1)
	for _, task := range tasks {
		assert.NotNil(t, task.GetAlpha())
	}
}

func TestGetZetaStageRatingsTask(t *testing.T) {
	CLIENT_ID := "test-client-id"
	ratings := []*model.Rating{
		{MovieId: "0", Rating: 4.5},
		{MovieId: "1", Rating: 3.8},
		{MovieId: "2", Rating: 5.0},
	}

	tasks := GetZetaStageRatingsTask(ratings, 2, CLIENT_ID, 0)

	assert.Len(t, tasks, 2) // Since joinersCount is 2, tasks are distributed into 2 groups
	for _, task := range tasks {
		assert.NotNil(t, task.GetZeta())
	}
}

func TestGetIotaStageCreditsTask(t *testing.T) {
	CLIENT_ID := "test-client-id"
	actors := []*model.Actor{
		{Id: "0", Name: "Actor 1", MovieId: "0"},
		{Id: "1", Name: "Actor 2", MovieId: "1"},
		{Id: "2", Name: "Actor 3", MovieId: "0"},
	}

	tasks := GetIotaStageCreditsTask(actors, 2, CLIENT_ID, 0)

	assert.Len(t, tasks, 2) // Since joinersCount is 2, tasks are distributed into 2 groups
	for _, task := range tasks {
		assert.NotNil(t, task.GetIota())
	}
}

func TestGetMuStageTask(t *testing.T) {
	CLIENT_ID := "test-client-id"
	movies := []*model.Movie{
		{Id: "0", Title: "Movie 1", Revenue: 1000000, Budget: 500000, Overview: "Overview 1"},
		{Id: "1", Title: "Movie 2", Revenue: 2000000, Budget: 1000000, Overview: "Overview 2"},
	}

	tasks := GetMuStageTask(movies, 2, CLIENT_ID, 0)

	assert.Len(t, tasks, 1)
	for _, task := range tasks {
		assert.NotNil(t, task.GetMu())
	}
}
