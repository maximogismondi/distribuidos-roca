package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	library "github.com/MaxiOtero6/TP-Distribuidos/client/src/client-library"
	"github.com/MaxiOtero6/TP-Distribuidos/client/src/client-library/model"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

const (
	RESET     = "\033[0m"
	BLACK     = "\033[30m"
	RED       = "\033[31m"
	GREEN     = "\033[32m"
	YELLOW    = "\033[33m"
	BLUE      = "\033[34m"
	MAGENTA   = "\033[35m"
	CYAN      = "\033[36m"
	WHITE     = "\033[37m"
	BOLD      = "\033[1m"
	DIM       = "\033[2m"
	ITALIC    = "\033[3m"
	UNDERLINE = "\033[4m"
	BLINK     = "\033[5m"
	REVERSE   = "\033[7m"
	HIDDEN    = "\033[8m"

	BG_BLACK   = "\033[40m"
	BG_RED     = "\033[41m"
	BG_GREEN   = "\033[42m"
	BG_YELLOW  = "\033[43m"
	BG_BLUE    = "\033[44m"
	BG_MAGENTA = "\033[45m"
	BG_CYAN    = "\033[46m"
	BG_WHITE   = "\033[47m"
)

var log = logging.MustGetLogger("log")

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("client")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("id")
	v.BindEnv("server", "address")
	v.BindEnv("log", "level")
	v.BindEnv("output", "file")

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// InitLogger Receives the log level to be set in go-logging as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	// Set the backends to be used.
	logging.SetBackend(backendLeveled)
	return nil
}

func handleSigterm(signalChan chan os.Signal, clientLibrary *library.Library) {
	s := <-signalChan

	clientLibrary.Stop()

	log.Infof("action: exit | result: success | signal: %v",
		s.String(),
	)
}

func logResults(results *model.Results) {
	if results == nil {
		log.Infof("action: processData | result: error | reason: results are nil")
		return
	}

	log.Infof(CYAN + UNDERLINE + "Query1 results:" + RESET)
	for index, movie := range results.Query1.Results {
		log.Infof(BOLD+"[%d] MovieId: %s | Title: %s | Genres: %v"+RESET, index+1, movie.MovieId, movie.Title, movie.Genres)
	}

	log.Infof(CYAN + UNDERLINE + "Query2 results:" + RESET)
	for position, country := range results.Query2.Results {
		log.Infof(BOLD+"[%d] Country: %s | TotalInvestment: %d"+RESET, position+1, country.Country, country.TotalInvestment)
	}

	log.Infof(CYAN + UNDERLINE + "Query3 results:" + RESET)
	for kind, data := range results.Query3.Results {
		log.Infof(BOLD+"[%v] Movie: %s | AvgRating: %.2f"+RESET, strings.ToUpper(kind), data.Title, data.AvgRating)
	}

	log.Infof(CYAN + UNDERLINE + "Query4 results:" + RESET)
	for position, actor := range results.Query4.Results {
		log.Infof(BOLD+"[%d] ActorId: %s | ActorName: %s | Participations: %v"+RESET, position+1, actor.ActorId, actor.ActorName, actor.Participations)
	}

	log.Infof(CYAN + UNDERLINE + "Query5 results:" + RESET)
	for sentiment, data := range results.Query5.Results {
		log.Infof(BOLD+"[%s] RevenueBudgetRatio: %.2f"+RESET, strings.ToUpper(sentiment), data.RevenueBudgetRatio)
	}
}

func main() {

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM)

	v, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
	}

	clientConfig := library.ClientConfig{
		ServerAddress:   v.GetString("server.address"),
		BatchMaxSize:    v.GetInt("batch.maxSize"),
		MoviesFilePath:  v.GetString("data.movies"),
		RatingsFilePath: v.GetString("data.ratings"),
		CreditsFilePath: v.GetString("data.credits"),
	}

	outputFilePath := v.GetString("output.file")

	clientLibrary, err := library.NewLibrary(clientConfig)
	if err != nil {
		log.Criticalf("%s", err)
	}

	go handleSigterm(signalChan, clientLibrary)

	results := clientLibrary.ProcessData()

	log.Infof("action: processData | result: success")

	clientLibrary.DumpResultsToJson(outputFilePath, results)

	logResults(results)

	clientLibrary.Stop()
}
