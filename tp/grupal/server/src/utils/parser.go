package utils

import (
	"encoding/csv"
	"regexp"
	"strconv"
	"strings"

	"github.com/MaxiOtero6/TP-Distribuidos/server/src/model"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// mapJsonRegex maps a JSON string using a regex pattern and returns the first group of each match.
func mapJsonRegex(json string, regex string) []string {
	re := regexp.MustCompile(regex)
	matches := re.FindAllStringSubmatch(json, -1)

	var mappedMatches []string
	for _, match := range matches {
		if len(match) > 1 {
			mappedMatches = append(mappedMatches, match[1])
		}
	}

	return mappedMatches
}

// mapJsonRegexTuple maps a JSON string using a regex pattern and returns the first group of each match as a tuple.
// It's the multiple item version of mapJsonRegex.
// If items is 0, it returns an empty slice.
// If items is greater than 0, it returns a slice of slices, where each inner slice contains the first 'items' groups of the match.
func mapJsonRegexTuple(json string, regex string, items int) [][]string {
	if items == 0 {
		return [][]string{}
	}

	re := regexp.MustCompile(regex)
	matches := re.FindAllStringSubmatch(json, -1)

	var mappedMatches [][]string
	for _, match := range matches {
		if len(match) > items {
			mappedMatches = append(mappedMatches, match[1:])
		}
	}

	if len(mappedMatches) == 0 {
		return nil
	}

	return mappedMatches
}

// ParseLine parses a CSV line and returns the fields as a slice of strings.
// It handles quoted fields, commas inside quotes, and escaped double quotes.
// It also handles JSON-like strings as single fields.
func ParseLine(line *string) (fields []string) {
	reader := csv.NewReader(strings.NewReader(*line))

	record, err := reader.Read()

	if err != nil {
		return []string{}
	}

	return record
}

// ParseMovie parses a movie line and returns a slice of DataRow with one item.
// fields is a slice of strings that contains the fields of the line.
// fields length must be 24 and not nil.
func ParseMovie(fields []string) []*model.Movie {
	// adult,belongs_to_collection,budget,genres,homepage,
	// id,imdb_id,original_language,original_title,overview,
	// popularity,poster_path,production_companies,production_countries,release_date,
	// revenue,runtime,spoken_languages,status,tagline,
	// title,video,vote_average,vote_count
	if fields == nil || len(fields) != 24 {
		return []*model.Movie{}
	}

	rawProdCountries := fields[13]
	rawGenres := fields[3]

	if len(rawProdCountries) == 0 || len(rawGenres) == 0 {
		return []*model.Movie{}
	}

	regex := `'name': '([^']+)'`
	prodCountries := mapJsonRegex(rawProdCountries, regex)
	genres := mapJsonRegex(rawGenres, regex)

	rawRevenue := fields[15]
	rawBudget := fields[2]

	revenue, err := strconv.ParseUint(rawRevenue, 10, 64)

	if err != nil {
		return []*model.Movie{}
	}

	budget, err := strconv.ParseUint(rawBudget, 10, 64)

	if err != nil {
		return []*model.Movie{}
	}

	rawReleaseDate := fields[14]
	rawReleaseYear := strings.Split(rawReleaseDate, "-")[0]
	releaseYear, err := strconv.ParseUint(rawReleaseYear, 10, 32)

	if err != nil {
		return []*model.Movie{}
	}

	id := fields[5]
	title := fields[20]
	overview := fields[9]

	if len(id) == 0 || len(title) == 0 || len(overview) == 0 {
		return []*model.Movie{}
	}

	return []*model.Movie{
		{
			Id:            id,
			ProdCountries: prodCountries,
			Title:         title,
			Revenue:       revenue,
			Budget:        budget,
			Overview:      overview,
			ReleaseYear:   uint32(releaseYear),
			Genres:        genres,
		},
	}
}

// ParseRating parses a rating line and returns a slice of DataRow with one item.
// fields is a slice of strings that contains the fields of the line.
// fields length must be 4 and not nil.
func ParseRating(fields []string) []*model.Rating {
	//userId,movieId,rating,timestamp
	if fields == nil || len(fields) != 4 {
		return []*model.Rating{}
	}

	rawRating := fields[2]

	rating, err := strconv.ParseFloat(rawRating, 32)

	if err != nil {
		return []*model.Rating{}
	}

	movieId := fields[1]

	if len(movieId) == 0 {
		return []*model.Rating{}
	}

	actorId := fields[0]

	if len(actorId) == 0 {
		return []*model.Rating{}
	}

	return []*model.Rating{
		{
			UserId:  actorId,
			MovieId: movieId,
			Rating:  float32(rating),
		},
	}
}

// ParseCredit parses a credit line and returns a slice of DataRow with many items as actors in the movie.
// It returns a slice of DataRow with one item for each actor.
// fields is a slice of strings that contains the fields of the line.
// fields length must be 3 and not nil.
func ParseCredit(fields []string) []*model.Actor {
	// cast,crew,id
	if fields == nil || len(fields) != 3 {
		return []*model.Actor{}
	}

	rawCast := fields[0]

	if len(rawCast) == 0 {
		return []*model.Actor{}
	}

	regex := `'id': (\d+).*?'name': ["']([^"']+)["']`
	cast := mapJsonRegexTuple(rawCast, regex, 2)

	var ret []*model.Actor

	for _, actor := range cast {
		id := actor[0]
		name := actor[1]
		movieId := fields[2]

		if len(id) == 0 || len(name) == 0 || len(movieId) == 0 {
			continue
		}

		ret = append(ret,
			&model.Actor{
				MovieId: movieId,
				Id:      id,
				Name:    name,
			},
		)
	}

	return ret
}
