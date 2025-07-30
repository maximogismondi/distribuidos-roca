package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	health_checker "github.com/MaxiOtero6/TP-Distribuidos/health/src"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

const NODE_TYPE = "HEALTH"

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("health")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("port")
	v.BindEnv("log", "level")

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead\n")
	}

	rabbitConfig := viper.New()
	rabbitConfig.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	rabbitConfig.SetConfigFile("./rabbitConfig.yaml")
	if err := rabbitConfig.ReadInConfig(); err != nil {
		fmt.Printf("Rabbit configuration could not be read.\n")
	}

	// Merge the rabbit-specific config with the main config
	if err := v.MergeConfigMap(rabbitConfig.AllSettings()); err != nil {
		return nil, fmt.Errorf("failed to merge rabbit-specific config: %w", err)
	}

	return v, nil
}

// InitLogger Receives the log level to be set in go-logging as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)

	formatStr := `%{time:2006-01-02 15:04:05} %{level:.5s}     ` + `%{message}`

	format := logging.MustStringFormatter(formatStr)
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

func initHealthChecker(v *viper.Viper, signalChan chan os.Signal) *health_checker.HealthChecker {
	id := v.GetString("id")

	clusterConfig := &model.WorkerClusterConfig{
		FilterCount:   v.GetInt("filter.count"),
		OverviewCount: v.GetInt("overview.count"),
		MapCount:      v.GetInt("map.count"),
		JoinCount:     v.GetInt("join.count"),
		ReduceCount:   v.GetInt("reduce.count"),
		MergeCount:    v.GetInt("merge.count"),
		TopCount:      v.GetInt("top.count"),
		HealthCount:   v.GetInt("health.count"),
		ServerCount:   v.GetInt("server.count"),
	}

	rabbitConfig := &model.RabbitConfig{
		FilterExchange:     v.GetString("consts.filterExchange"),
		OverviewExchange:   v.GetString("consts.overviewExchange"),
		MapExchange:        v.GetString("consts.mapExchange"),
		JoinExchange:       v.GetString("consts.joinExchange"),
		ReduceExchange:     v.GetString("consts.reduceExchange"),
		MergeExchange:      v.GetString("consts.mergeExchange"),
		TopExchange:        v.GetString("consts.topExchange"),
		ResultExchange:     v.GetString("consts.resultExchange"),
		EofExchange:        v.GetString("consts.eofExchange"),
		BroadcastID:        v.GetString("consts.broadcastId"),
		EofBroadcastRK:     v.GetString("consts.eofBroadcastRK"),
		ControlExchange:    v.GetString("consts.controlExchange"),
		ControlBroadcastRK: v.GetString("consts.controlBroadcastRK"),
		LeaderRK:           v.GetString("consts.leaderRK"),
		HealthExchange:     v.GetString("consts.healthExchange"),
	}

	infraConfig := model.NewInfraConfig(id, clusterConfig, rabbitConfig, "", 0)

	log.Debugf("InfraConfig:\n\tWorkersConfig:%v\n\tRabbitConfig:%v", infraConfig.GetWorkers(), infraConfig.GetRabbit())

	exchanges, queues, _, err := utils.GetRabbitConfig(NODE_TYPE, v)

	qName := fmt.Sprintf("%s_%s_queue", NODE_TYPE, id)
	healthQName := "health_" + qName
	controlQName := "control_" + qName

	queues = append(queues, map[string]string{
		"name": healthQName,
		"ttl":  "4000", // 4 seconds
	})

	queues = append(queues, map[string]string{
		"name": controlQName,
	})

	binds := make([]map[string]string, 0)
	binds = append(binds, map[string]string{
		"queue":    queues[0]["name"],
		"exchange": infraConfig.GetControlExchange(),
		"extraRK":  infraConfig.GetLeaderRK(),
	})

	binds = append(binds, map[string]string{
		"queue":    healthQName,
		"exchange": infraConfig.GetHealthExchange(),
	})

	binds = append(binds, map[string]string{
		"queue":    controlQName,
		"exchange": infraConfig.GetControlExchange(),
		"extraRK":  infraConfig.GetControlBroadcastRK(),
	})

	if err != nil {
		log.Panicf("Failed to parse RabbitMQ configuration: %s", err)
	}

	hc := health_checker.NewHealthChecker(
		id,
		v.GetInt("healthCheckInterval"),
		infraConfig,
		queues[0]["name"],
		v.GetUint32("healthMaxStatus"),
		signalChan,
		v.GetInt("healthElectionTimeout"),
		v.GetString("name"),
	)
	hc.InitConfig(exchanges, queues, binds)

	log.Infof("Server '%v' ready", hc.ID)

	return hc
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

	hc := initHealthChecker(v, signalChan)
	hc.Run()

	close(signalChan)
}
