package utils

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"

	"github.com/MaxiOtero6/TP-Distribuidos/client/src/client-library/model"
)

type q1 struct {
	Title  string `json:"title"`
	Genres string `json:"genres"`
}

type q2 map[string]uint64

type q3Data struct {
	Id     uint32  `json:"id"`
	Title  string  `json:"title"`
	Rating float32 `json:"rating"`
}
type q3 map[string]q3Data

type q5 map[string]float32

type res struct {
	Query1 []*q1           `json:"Query1"`
	Query2 q2              `json:"Query2"`
	Query3 q3              `json:"Query3"`
	Query4 []*model.Query4 `json:"Query4"`
	Query5 q5              `json:"Query5"`
}

func stringSliceToJSONString(s []string) string {
	var buffer bytes.Buffer

	buffer.WriteString("[")
	for i, str := range s {
		quotedStr, err := json.Marshal(str) // Properly quote the string
		if err != nil {
			log.Warningf("Error marshalling string '%s': %v", str, err)
			return ""
		}
		singleQuotedStr := strings.ReplaceAll(string(quotedStr), `"`, "'")

		buffer.WriteString(singleQuotedStr)
		if i < len(s)-1 {
			buffer.WriteString(", ")
		}
	}

	buffer.WriteString("]")

	return buffer.String()
}

func adaptResults(results *model.Results) *res {

	q1Data := make([]*q1, len(results.Query1.Results))
	for i, data := range results.Query1.Results {
		q1Data[i] = &q1{
			Title:  data.Title,
			Genres: stringSliceToJSONString(data.Genres),
		}
	}

	q2 := make(q2)
	for _, country := range results.Query2.Results {
		q2[country.Country] = country.TotalInvestment
	}

	q3 := make(q3)
	for kind, data := range results.Query3.Results {
		q3[kind] = q3Data{
			Id:     0,
			Title:  data.Title,
			Rating: data.AvgRating,
		}
	}

	q5 := make(q5)
	for sentiment, data := range results.Query5.Results {
		q5[sentiment] = data.RevenueBudgetRatio
	}

	return &res{
		Query1: q1Data,
		Query2: q2,
		Query3: q3,
		Query4: results.Query4.Results,
		Query5: q5,
	}
}

func DumpResultsToJson(filePath string, results *model.Results) {
	file, err := os.Create(filePath)
	if err != nil {
		log.Errorf("Error creating file: %v", err)
	}

	defer file.Close()

	encoder := json.NewEncoder(file)

	// Optional: Set indentation for more readable JSON output
	encoder.SetIndent("", "  ")

	if results == nil {
		log.Warningf("Results are nil, nothing to write to file '%s'", filePath)
		return
	}
	adaptedResults := adaptResults(results)

	err = encoder.Encode(adaptedResults)

	if err != nil {
		log.Errorf("Error encoding JSON to file '%s': %v", filePath, err)
	} else {
		log.Infof("Results successfully written to %s", filePath)
	}
}
