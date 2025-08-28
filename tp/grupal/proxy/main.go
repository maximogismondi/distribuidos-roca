package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	proxy "github.com/MaxiOtero6/TP-Distribuidos/proxy/src"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

var log = logging.MustGetLogger("log")

const NODE_TYPE = "PROXY"

type Instance interface {
	Run() error
	Stop()
}

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("proxy")
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

	return v, nil
}

// InitLogger Receives the log level to be set in go-logging as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string, isChild bool) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)

	prefix := ""
	if isChild {
		prefix = "Handler [%{pid}] | "
	}

	formatStr := `%{time:2006-01-02 15:04:05} %{level:.5s}     ` + prefix + `%{message}`

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

func handleSigterm(signalChan chan os.Signal, stoppable Instance, wg *sync.WaitGroup) {
	defer wg.Done()
	s := <-signalChan

	if s != nil {
		stoppable.Stop()
		log.Infof("action: exit | result: success | signal: %v",
			s.String(),
		)
	}
}

func initConnectionHandler(v *viper.Viper) *proxy.Proxy {
	proxyAddress := v.GetString("address")

	serverAddresses := loadServerConfig(v)

	s, err := proxy.NewConnectionHandler(NODE_TYPE, proxyAddress, serverAddresses)

	if err != nil {
		log.Panicf("Failed to initialize proxy: %s", err)
	}

	log.Infof("Proxy '%v' ready", s.ID)

	return s
}

func loadServerConfig(v *viper.Viper) []string {
	serverAmount := v.GetInt("server.count")
	if serverAmount == 0 {
		log.Panicf("Server amount not set")
	}

	serverAddress := make([]string, 0, serverAmount)
	basePort := 8080
	for i := 0; i < serverAmount; i++ {
		port := basePort + i
		serverAddress = append(serverAddress, fmt.Sprintf("server_%d:%d", i, port))
	}

	return serverAddress
}

func initHandler(v *viper.Viper, addressIndex string) *proxy.Handler {
	const SOCKET_FD uintptr = 3

	serverAddresses := loadServerConfig(v)

	handler, err := proxy.NewHandler(serverAddresses, addressIndex, SOCKET_FD)
	if err != nil {
		log.Panicf("Failed to initialize client handler: %s", err)
	}

	return handler
}

func main() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM)

	var wg sync.WaitGroup

	v, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}
	isChild := len(os.Args) == 3 && os.Args[1] == "child"

	if err := InitLogger(v.GetString("log.level"), isChild); err != nil {
		log.Criticalf("%s", err)
	}

	var instance Instance

	if len(os.Args) == 1 {
		instance = initConnectionHandler(v)
	} else if isChild {

		serverIndex := os.Args[2]
		instance = initHandler(v, serverIndex)

	} else {
		log.Panicf("Bad call to server. Usage for server: %s ; Usage for clientHandler: %s child", os.Args[0], os.Args[0])
	}

	wg.Add(1)
	go handleSigterm(signalChan, instance, &wg)

	instance.Run()

	close(signalChan)

	wg.Wait()

}
