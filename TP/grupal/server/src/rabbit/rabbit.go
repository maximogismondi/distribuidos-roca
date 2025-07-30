package rabbit

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/mom"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	common_model "github.com/MaxiOtero6/TP-Distribuidos/common/model"
	common_utils "github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/model"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/utils"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

var log = logging.MustGetLogger("log")

// RabbitHandler handles the RabbitMQ connection and message publishing for the server
type RabbitHandler struct {
	rabbitMQ           *mom.RabbitMQ
	infraConfig        *common_model.InfraConfig
	resultQueueName    string
	resultExchangeName string
}

// NewRabbitHandler creates a new RabbitHandler instance
func NewRabbitHandler(infraConfig *common_model.InfraConfig) *RabbitHandler {
	return &RabbitHandler{
		rabbitMQ:    mom.NewRabbitMQ(),
		infraConfig: infraConfig,
	}
}

// InitConfig initializes the RabbitMQ connection
// and sets up the exchanges, queues, and bindings
func (r *RabbitHandler) InitConfig(id string, exchanges []map[string]string, queues []map[string]string, binds []map[string]string) {
	if len(binds) != 1 {
		log.Panicf("Expected exactly one binding to the results queue for servers, but got %d", len(binds))
	}

	// Do not bind the server to a queue without some clientId as routing key.
	// This routingKey param does nothing
	r.rabbitMQ.InitConfig(exchanges, queues, nil, id)

	r.resultQueueName = binds[0]["queue"]
	r.resultExchangeName = binds[0]["exchange"]
}

// Register a new client with the server
// The client is identified by its ID
// The server binds the client ID to the result queue and exchange
func (r *RabbitHandler) RegisterNewClient(clientId string) {
	// Check if the result exchange name is empty.
	// Do not check if the result queue name is empty, because it can be an anonymous queue
	if len(r.resultExchangeName) == 0 {
		log.Panicf("Result exchange name is empty, do you call InitConfig?")
	}

	r.resultQueueName = "client_" + clientId

	args := map[string]string{
		"expires": r.infraConfig.GetClientQueueTTL(),
	}

	r.rabbitMQ.NewQueue(r.resultQueueName, args)
	r.rabbitMQ.BindQueue(r.resultQueueName, r.resultExchangeName, clientId)
}

// Close closes the RabbitMQ connection
// and the channel used for consuming messages
func (r *RabbitHandler) Close() {
	r.rabbitMQ.Close()
}

// SendMoviesRabbit sends the movies to the filter and overview exchanges
func (r *RabbitHandler) SendMoviesRabbit(movies []*model.Movie, clientId string, taskId uint32) {
	FILTER_COUNT := r.infraConfig.GetFilterCount()
	OVERVIEW_COUNT := r.infraConfig.GetOverviewCount()

	FILTER_EXCHANGE := r.infraConfig.GetFilterExchange()
	OVERVIEW_EXCHANGE := r.infraConfig.GetOverviewExchange()

	alphaTasks := utils.GetAlphaStageTask(movies, FILTER_COUNT, clientId, taskId)
	muTasks := utils.GetMuStageTask(movies, OVERVIEW_COUNT, clientId, taskId)
	gammaTasks := utils.GetGammaStageTask(movies, FILTER_COUNT, clientId, taskId)

	r.publishTasksRabbit(alphaTasks, FILTER_EXCHANGE)
	r.publishTasksRabbit(muTasks, OVERVIEW_EXCHANGE)
	r.publishTasksRabbit(gammaTasks, FILTER_EXCHANGE)
}

// SendMoviesEOFRabbit sends the EOF message to the filter and overview exchanges
func (r *RabbitHandler) SendMoviesEOFRabbit(clientId string, taskCount uint32) {
	FILTER_COUNT := r.infraConfig.GetFilterCount()
	OVERVIEW_COUNT := r.infraConfig.GetOverviewCount()

	FILTER_EXCHANGE := r.infraConfig.GetFilterExchange()
	OVERVIEW_EXCHANGE := r.infraConfig.GetOverviewExchange()

	FILTER_ROUTING_KEY := r.infraConfig.GetEofBroadcastRK()
	OVERVIEW_ROUTING_KEY := r.infraConfig.GetEofBroadcastRK()

	r.publishTasksRabbit(utils.GetEOFTask(FILTER_COUNT, clientId, utils.ALPHA_STAGE, FILTER_ROUTING_KEY, taskCount), FILTER_EXCHANGE)
	r.publishTasksRabbit(utils.GetEOFTask(OVERVIEW_COUNT, clientId, utils.MU_STAGE, OVERVIEW_ROUTING_KEY, taskCount), OVERVIEW_EXCHANGE)
	r.publishTasksRabbit(utils.GetEOFTask(FILTER_COUNT, clientId, utils.GAMMA_STAGE, FILTER_ROUTING_KEY, taskCount), FILTER_EXCHANGE)
}

// SendRatingsRabbit sends the ratings to the join exchange
// The ratings are shuffled by the join count hashing
func (r *RabbitHandler) SendRatingsRabbit(ratings []*model.Rating, clientId string, taskId uint32) {
	JOINER_COUNT := r.infraConfig.GetJoinCount()
	JOINER_EXCHANGE := r.infraConfig.GetJoinExchange()

	zetaTasks := utils.GetZetaStageRatingsTask(ratings, JOINER_COUNT, clientId, taskId)
	r.publishTasksRabbit(zetaTasks, JOINER_EXCHANGE)
}

// SendRatingsEOFRabbit sends the EOF message for ratings to the join exchange
func (r *RabbitHandler) SendRatingsEOFRabbit(clientId string, taskCount uint32) {
	JOINER_COUNT := r.infraConfig.GetJoinCount()
	JOINER_EXCHANGE := r.infraConfig.GetJoinExchange()
	JOINER_ROUTING_KEY := common_utils.GetWorkerIdFromHash(JOINER_COUNT, clientId+utils.ZETA_STAGE)

	eofTasks := utils.GetEOFTask(JOINER_COUNT, clientId, utils.ZETA_STAGE, JOINER_ROUTING_KEY, taskCount)
	r.publishTasksRabbit(eofTasks, JOINER_EXCHANGE)
}

// SendActorsRabbit sends the actors to the join exchange
// The actors are shuffled by the join count hashing
func (r *RabbitHandler) SendActorsRabbit(actors []*model.Actor, clientId string, taskId uint32) {
	JOINER_COUNT := r.infraConfig.GetJoinCount()
	JOINER_EXCHANGE := r.infraConfig.GetJoinExchange()

	iotaTasks := utils.GetIotaStageCreditsTask(actors, JOINER_COUNT, clientId, taskId)
	r.publishTasksRabbit(iotaTasks, JOINER_EXCHANGE)
}

// SendActorsEOFRabbit sends the EOF message for actors to the join exchange
func (r *RabbitHandler) SendActorsEOFRabbit(clientId string, taskCount uint32) {
	JOINER_COUNT := r.infraConfig.GetJoinCount()
	JOINER_EXCHANGE := r.infraConfig.GetJoinExchange()
	JOINER_ROUTING_KEY := common_utils.GetWorkerIdFromHash(JOINER_COUNT, clientId+utils.IOTA_STAGE)

	r.publishTasksRabbit(utils.GetEOFTask(JOINER_COUNT, clientId, utils.IOTA_STAGE, JOINER_ROUTING_KEY, taskCount), JOINER_EXCHANGE)
}

// publishTasksRabbit publishes the tasks to the specified exchange
// The routing key is the task key in the tasks map
func (r *RabbitHandler) publishTasksRabbit(tasks map[string]*protocol.Task, exchange string) {
	for routingKey, task := range tasks {
		bytes, err := proto.Marshal(task)

		if err != nil {
			log.Errorf("Error marshalling %T task: %v", task.GetStage(), err)
			continue
		}

		r.rabbitMQ.Publish(exchange, routingKey, bytes)
	}
}

// GetResults retrieves the results from the result queue
// The results are unmarshalled and returned as a ResultsResponse
func (r *RabbitHandler) GetResults(clientId string) (*protocol.ResultsResponse, *amqp.Delivery) {
	unmarshallResult := func(msg amqp.Delivery) (*protocol.ResultsResponse_Result, error) {
		task := &protocol.Task{}
		err := proto.Unmarshal(msg.Body, task)

		if err != nil {
			return nil, err
		}

		result := &protocol.ResultsResponse_Result{
			TaskIdentifier: task.GetTaskIdentifier(),
		}

		switch task.GetStage().(type) {
		case *protocol.Task_Result1:
			result.Message = &protocol.ResultsResponse_Result_Result1{
				Result1: task.GetResult1(),
			}
		case *protocol.Task_Result2:
			result.Message = &protocol.ResultsResponse_Result_Result2{
				Result2: task.GetResult2(),
			}
		case *protocol.Task_Result3:
			result.Message = &protocol.ResultsResponse_Result_Result3{
				Result3: task.GetResult3(),
			}
		case *protocol.Task_Result4:
			result.Message = &protocol.ResultsResponse_Result_Result4{
				Result4: task.GetResult4(),
			}
		case *protocol.Task_Result5:
			result.Message = &protocol.ResultsResponse_Result_Result5{
				Result5: task.GetResult5(),
			}
		case *protocol.Task_OmegaEOF:
			result.Message = &protocol.ResultsResponse_Result_OmegaEOF{
				OmegaEOF: task.GetOmegaEOF(),
			}
		default:
			return nil, fmt.Errorf("unknown task stage: %v", task.GetStage())
		}

		return result, nil
	}

	results := &protocol.ResultsResponse{
		Results: make([]*protocol.ResultsResponse_Result, 0),
		Status:  protocol.MessageStatus_PENDING,
	}

	msg, ok := r.rabbitMQ.GetHeadDelivery(r.resultQueueName)

	if !ok {
		return results, nil
	}

	result, err := unmarshallResult(msg)

	if err != nil {
		log.Errorf("Error unmarshalling task: %v", err)
		return results, nil
	}

	results.Results = append(results.Results, result)
	//msg.Ack(false)

	if len(results.Results) > 0 {
		results.Status = protocol.MessageStatus_SUCCESS
	}

	return results, &msg
}

func (r *RabbitHandler) AckResults(msg *amqp.Delivery) {
	if msg != nil {
		msg.Ack(false)
	}
}

func (r *RabbitHandler) RemoveClient(clientId string) {
	r.rabbitMQ.DeleteQueue(r.resultQueueName)
}
