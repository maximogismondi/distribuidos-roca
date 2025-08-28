package utils

import (
	"testing"

	"github.com/MaxiOtero6/TP-Distribuidos/server/src/model"
	"github.com/stretchr/testify/assert"
)

const MOVIE_LINE = `False,"{'id': 10194, 'name': 'Toy Story Collection', 'poster_path': '/7G9915LfUQ2lVfwMEEhDsn3kT4B.jpg', 'backdrop_path': '/9FBwqcd9IRruEDUrTdcaafOMKUq.jpg'}",30000000,"[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]",http://toystory.disney.com/toy-story,862,tt0114709,en,Toy Story,"Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody from their owner, the duo eventually learns to put aside their differences.",21.946943,/rhIRbceoE9lR4veEXuwCC2wARtG.jpg,"[{'name': 'Pixar Animation Studios', 'id': 3}]","[{'iso_3166_1': 'US', 'name': 'United States of America'}]",1995-10-30,373554033,81.0,"[{'iso_639_1': 'en', 'name': 'English'}]",Released,,Toy Story,False,7.7,5415`
const RATING_LINE = `1,110,1.0,1425941529`
const CREDIT_LINE = `"[{'cast_id': 14, 'character': 'Woody (voice)', 'credit_id': '52fe4284c3a36847f8024f95', 'gender': 2, 'id': 31, 'name': 'Tom Hanks', 'order': 0, 'profile_path': '/pQFoyx7rp09CJTAb932F2g8Nlho.jpg'}, {'cast_id': 15, 'character': 'Buzz Lightyear (voice)', 'credit_id': '52fe4284c3a36847f8024f99', 'gender': 2, 'id': 12898, 'name': 'Tim Allen', 'order': 1, 'profile_path': '/uX2xVf6pMmPepxnvFWyBtjexzgY.jpg'}]","[{'credit_id': '52fe4284c3a36847f8024f49', 'department': 'Directing', 'gender': 2, 'id': 7879, 'job': 'Director', 'name': 'John Lasseter', 'profile_path': '/7EdqiNbr4FRjIhKHyPPdFfEEEFG.jpg'}, {'credit_id': '52fe4284c3a36847f8024f4f', 'department': 'Writing', 'gender': 2, 'id': 12891, 'job': 'Screenplay', 'name': 'Joss Whedon', 'profile_path': '/dTiVsuaTVTeGmvkhcyJvKp2A5kr.jpg'}]",862`

func TestMapJsonRegex(t *testing.T) {
	t.Run("TestGetNamesFromJsonGenreObjectList", func(t *testing.T) {
		json := "[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"
		rx := `'name': '([^']+)'`

		expected := []string{"Animation", "Comedy", "Family"}
		actual := mapJsonRegex(json, rx)

		assert.Equal(t, len(expected), len(actual), "Expected %d items, but got %d", len(expected), len(actual))
		assert.EqualValues(t, expected, actual, "Expected %v, but got %v", expected, actual)
	})

	t.Run("TestGetIdsFromJsonGenreObjectList", func(t *testing.T) {
		json := "[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"
		rx := `'id': (\d+)`

		expected := []string{"16", "35", "10751"}
		actual := mapJsonRegex(json, rx)

		assert.Equal(t, len(expected), len(actual), "Expected %d items, but got %d", len(expected), len(actual))
		assert.EqualValues(t, expected, actual, "Expected %v, but got %v", expected, actual)
	})

	t.Run("TestGetNonExistentFieldFromJsonGenreObjectList", func(t *testing.T) {
		json := "[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"
		rx := `'nonExistentField': '([^']+)'`

		actual := mapJsonRegex(json, rx)

		assert.Empty(t, actual, "Expected an empty slice, but got %v", actual)
	})
}

func TestMapJsonRegexTuple(t *testing.T) {
	t.Run("TestGetIdsAndNamesFromJsonGenreObjectList", func(t *testing.T) {
		json := "[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"
		rx := `'id': (\d+).*?'name': '([^']+)'`

		expected := [][]string{
			{"16", "Animation"},
			{"35", "Comedy"},
			{"10751", "Family"},
		}
		actual := mapJsonRegexTuple(json, rx, 2)

		assert.EqualValues(t, expected, actual, "Expected %v, but got %v", expected, actual)
	})

	t.Run("TestGetIdsFromJsonGenreObjectList", func(t *testing.T) {
		json := "[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"
		rx := `'id': (\d+)`

		expected := [][]string{
			{"16"},
			{"35"},
			{"10751"},
		}
		actual := mapJsonRegexTuple(json, rx, 1)

		assert.EqualValues(t, expected, actual, "Expected %v, but got %v", expected, actual)
	})

	t.Run("TestGetNonExistentFieldFromJsonGenreObjectList", func(t *testing.T) {
		json := "[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"
		rx := `'nonExistentField': '([^']+)'`

		actual := mapJsonRegexTuple(json, rx, 1)

		assert.Empty(t, actual, "Expected an empty slice, but got %v", len(actual))
	})

	t.Run("TestGetZeroFieldsWithExistentFieldsRegexFromJsonGenreObjectList", func(t *testing.T) {
		json := "[{'id': 16, 'name': 'Animation'}, {'id': 35, 'name': 'Comedy'}, {'id': 10751, 'name': 'Family'}]"
		rx := `'id': (\d+)`

		actual := mapJsonRegexTuple(json, rx, 0)

		assert.Empty(t, actual, "Expected an empty slice, but got %v", len(actual))
	})
}

func TestParseLine(t *testing.T) {
	t.Run("TestParseLineWithComma", func(t *testing.T) {
		line := `1,"John, Doe",25`
		expected := []string{"1", "John, Doe", "25"}
		actual := ParseLine(&line)

		assert.EqualValues(t, expected, actual, "Expected %v, but got %v", expected, actual)
	})

	t.Run("TestParseLineWithQuotes", func(t *testing.T) {
		line := `1,"John Doe, a great person",25`
		expected := []string{"1", "John Doe, a great person", "25"}
		actual := ParseLine(&line)

		assert.EqualValues(t, expected, actual, "Expected %v, but got %v", expected, actual)
	})

	t.Run("TestParseLineWithEmptyFields", func(t *testing.T) {
		line := `1,"",`
		expected := []string{"1", "", ""}
		actual := ParseLine(&line)

		assert.EqualValues(t, expected, actual, "Expected %v, but got %v", expected, actual)
	})

	t.Run("TestParseLineWithJsonObjects", func(t *testing.T) {
		line := `1,"{'id': 16, 'name': 'Animation'}",25`
		expected := []string{"1", "{'id': 16, 'name': 'Animation'}", "25"}
		actual := ParseLine(&line)

		assert.EqualValues(t, expected, actual, "Expected %v, but got %v", expected, actual)
	})
}

func TestParseMovie(t *testing.T) {

	line := MOVIE_LINE
	fields := ParseLine(&line)

	t.Run("TestParseToyStoryMovie", func(t *testing.T) {
		expected := []*model.Movie{
			{
				Id:            "862",
				ProdCountries: []string{"United States of America"},
				Title:         "Toy Story",
				Revenue:       373554033,
				Budget:        30000000,
				Overview:      "Led by Woody, Andy's toys live happily in his room until Andy's birthday brings Buzz Lightyear onto the scene. Afraid of losing his place in Andy's heart, Woody plots against Buzz. But when circumstances separate Buzz and Woody from their owner, the duo eventually learns to put aside their differences.",
				ReleaseYear:   1995,
				Genres:        []string{"Animation", "Comedy", "Family"},
			},
		}

		actual := ParseMovie(fields)

		assert.EqualValues(t, expected, actual, "Expected %v, but got %v", expected, actual)
	})

	t.Run("TestParseMovieWithEmptyFields", func(t *testing.T) {
		actual := ParseMovie([]string{})

		assert.Empty(t, actual, "Expected zero items, but got %v", actual)
	})

	t.Run("TestParseMovieWithWrongFieldsLength", func(t *testing.T) {
		actual := ParseMovie(fields[:10])

		assert.Empty(t, actual, "Expected zero items, but got %v", actual)
	})

	t.Run("TestParseMovieWithNilFields", func(t *testing.T) {
		actual := ParseMovie(nil)

		assert.Empty(t, actual, "Expected zero items, but got %v", actual)
	})

	t.Run("TestParseMovieWithWrongFormatFieldsNonNumericRevenue", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[15] = "nonNumeric" //revenue

		actual := ParseMovie(fields2)

		assert.Empty(t, actual, "Expected zero items, but got %d", len(actual))
	})

	t.Run("TestParseMovieWithWrongFormatFieldsNonNumericBudget", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[2] = "nonNumeric" //budget

		actual := ParseMovie(fields2)

		assert.Empty(t, actual, "Expected zero items, but got %d", len(actual))
	})

	t.Run("TestParseMovieWithWrongFormatFieldsNonNumericReleaseYear", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[14] = "nonNumeric" //release date

		actual := ParseMovie(fields2)

		assert.Empty(t, actual, "Expected zero items, but got %d", len(actual))
	})

	t.Run("TestParseMovieWithWrongFormatFieldsEmptyProdCountries", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[13] = "" //prod countries

		actual := ParseMovie(fields2)

		assert.Empty(t, actual, "Expected zero items, but got %d", len(actual))
	})

	t.Run("TestParseMovieWithWrongFormatFieldsEmptyGenres", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[3] = "" //genres

		actual := ParseMovie(fields2)

		assert.Empty(t, actual, "Expected zero items, but got %d", len(actual))
	})

	t.Run("TestParseMovieWithWrongFormatFieldsEmptyId", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[5] = "" //id

		actual := ParseMovie(fields2)

		assert.Empty(t, actual, "Expected zero items, but got %d", len(actual))
	})

	t.Run("TestParseMovieWithWrongFormatFieldsEmptyTitle", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[20] = "" //title

		actual := ParseMovie(fields2)

		assert.Empty(t, actual, "Expected zero items, but got %d", len(actual))
	})

	t.Run("TestParseMovieWithWrongFormatFieldsEmptyOverview", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[9] = "" //overview

		actual := ParseMovie(fields2)

		assert.Empty(t, actual, "Expected zero items, but got %d", len(actual))
	})
}

func TestParseRating(t *testing.T) {
	line := RATING_LINE
	fields := ParseLine(&line)

	t.Run("TestParseRating", func(t *testing.T) {
		expected := []*model.Rating{
			{
				MovieId: "110",
				Rating:  1.0,
			},
		}

		actual := ParseRating(fields)

		assert.EqualValues(t, expected, actual, "Expected %v, but got %v", expected, actual)
	})

	t.Run("TestParseCreditWithEmptyFields", func(t *testing.T) {
		actual := ParseRating([]string{})

		assert.Empty(t, actual, "Expected zero items, but got %v", actual)
	})

	t.Run("TestParseCreditWithWrongFieldsLength", func(t *testing.T) {
		actual := ParseRating(fields[:1])

		assert.Empty(t, actual, "Expected zero items, but got %v", actual)
	})

	t.Run("TestParseCreditWithNilFields", func(t *testing.T) {
		actual := ParseRating(nil)

		assert.Empty(t, actual, "Expected zero items, but got %v", actual)
	})

	t.Run("TestParseRatingWithWrongFormatFieldsNonNumericRating", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[2] = "nonNumeric" //rating

		actual := ParseRating(fields2)

		assert.Empty(t, actual, "Expected zero items, but got %v", actual)
	})

	t.Run("TestParseCreditWithWrongFormatFieldsEmptyMovieId", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[1] = "" //movieId

		actual := ParseRating(fields2)

		assert.Empty(t, actual, "Expected zero items, but got %v", actual)
	})
}

func TestParseCredits(t *testing.T) {
	line := CREDIT_LINE
	fields := ParseLine(&line)

	t.Run("TestParseCredit", func(t *testing.T) {
		var expected []*model.Actor

		expected = append(expected, &model.Actor{
			MovieId: "862",
			Id:      "31",
			Name:    "Tom Hanks",
		})

		expected = append(expected, &model.Actor{
			MovieId: "862",
			Id:      "12898",
			Name:    "Tim Allen",
		})

		actual := ParseCredit(fields)

		assert.EqualValues(t, expected, actual, "Expected %s, but got %s", expected, actual)
	})

	t.Run("TestParseRatingWithEmptyFields", func(t *testing.T) {
		actual := ParseCredit([]string{})

		assert.Empty(t, actual, "Expected zero items, but got %v", actual)
	})

	t.Run("TestParseRatingWithWrongFieldsLength", func(t *testing.T) {
		actual := ParseCredit(fields[:1])

		assert.Empty(t, actual, "Expected zero items, but got %v", actual)
	})

	t.Run("TestParseRatingWithNilFields", func(t *testing.T) {
		actual := ParseCredit(nil)

		assert.Empty(t, actual, "Expected zero items, but got %v", actual)
	})

	t.Run("TestParseRatingWithWrongFormatFieldsEmptyMovieId", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[2] = "" //movieId

		actual := ParseCredit(fields2)

		assert.Empty(t, actual, "Expected zero items, but got %v", actual)
	})

	t.Run("TestParseCreditWithWrongFormatFieldsEmptyId", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[0] = `"[{'id': , 'name': 'Tom Hanks'}, {'id': 12898, 'name': 'Tim Allen'}]"` //id
		expected := []*model.Actor{{
			MovieId: "862",
			Id:      "12898",
			Name:    "Tim Allen",
		}}

		actual := ParseCredit(fields2)

		assert.Len(t, actual, 1, "Expected one item, but got %v", len(actual))
		assert.EqualValues(t, expected, actual, "Expected %s, but got %s", expected, actual)
	})

	t.Run("TestParseCreditWithWrongFormatFieldsEmptyName", func(t *testing.T) {
		var fields2 []string = make([]string, len(fields))
		copy(fields2, fields)

		fields2[0] = `"[{'id': 31, 'name': 'Tom Hanks'}, {'id': 12898, 'name': }]"` //name
		expected := []*model.Actor{{
			MovieId: "862",
			Id:      "31",
			Name:    "Tom Hanks",
		}}

		actual := ParseCredit(fields2)

		assert.Len(t, actual, 1, "Expected one item, but got %v", len(actual))
		assert.EqualValues(t, expected, actual, "Expected %s, but got %s", expected, actual)
	})
}

// func TestParseRow(t *testing.T) {
// 	t.Run("TestParseRowWithMovie", func(t *testing.T) {
// 		line := MOVIE_LINE
// 		actual, err := ParseRow(&line, protocol.FileType_MOVIES)

// 		if err != nil {
// 			t.Errorf("Unexpected error: %v", err)
// 		}

// 		if len(actual) != 1 {
// 			t.Errorf("Expected 1 item, but got %d", len(actual))
// 		}

// 		if actual[0].Data.(*protocol.DataRow_Movie) == nil {
// 			t.Errorf("Expected DataRow_Movie, but got %T", actual[0].Data)
// 		}
// 	})

// 	t.Run("TestParseRowWithRating", func(t *testing.T) {
// 		line := RATING_LINE

// 		actual, err := ParseRow(&line, protocol.FileType_RATINGS)

// 		if err != nil {
// 			t.Errorf("Unexpected error: %v", err)
// 		}

// 		if len(actual) != 1 {
// 			t.Errorf("Expected 1 item, but got %d", len(actual))
// 		}

// 		if actual[0].Data.(*protocol.DataRow_Rating) == nil {
// 			t.Errorf("Expected DataRow_Movie, but got %T", actual[0].Data)
// 		}
// 	})

// 	t.Run("TestParseRowWithCredits", func(t *testing.T) {
// 		line := CREDIT_LINE

// 		actual, err := ParseRow(&line, protocol.FileType_CREDITS)

// 		if err != nil {
// 			t.Errorf("Unexpected error: %v", err)
// 		}

// 		if len(actual) != 2 {
// 			t.Errorf("Expected 2 item, but got %d", len(actual))
// 		}

// 		if actual[0].Data.(*protocol.DataRow_Credit) == nil {
// 			t.Errorf("Expected DataRow_Movie, but got %T", actual[0].Data)
// 		}

// 		if actual[1].Data.(*protocol.DataRow_Credit) == nil {
// 			t.Errorf("Expected DataRow_Movie, but got %T", actual[1].Data)
// 		}
// 	})

// 	t.Run("TestParseEOFLine", func(t *testing.T) {
// 		line := ""
// 		actual, err := ParseRow(&line, protocol.FileType_EOF)

// 		if err != nil {
// 			t.Errorf("Unexpected error: %v", err)
// 		}

// 		if actual != nil {
// 			t.Errorf("Expected nil, but got %v", actual)
// 		}
// 	})

// 	t.Run("TestParseRowWithInvalidFileType", func(t *testing.T) {
// 		line := MOVIE_LINE
// 		actual, err := ParseRow(&line, protocol.FileType(999))

// 		if err == nil {
// 			t.Errorf("Expected error, but got nil")
// 		}

// 		if actual != nil {
// 			t.Errorf("Expected nil, but got %v", actual)
// 		}
// 	})
// }
