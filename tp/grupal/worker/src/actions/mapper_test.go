package actions

// func TestMapper(t *testing.T) {
// 	BROADCAST_ID := ""
// 	CLIENT_ID := "testClientId"

// 	var testInfraConfig = model.NewInfraConfig(
// 		BROADCAST_ID,
// 		&model.WorkerClusterConfig{
// 			ReduceCount: 2,
// 		},
// 		&model.RabbitConfig{
// 			MapExchange:    "mapExchange",
// 			ReduceExchange: "reduceExchange",
// 			BroadcastID:    "",
// 		},
// 		"",
// 	)

// 	itemHash := func(workersCount int, item string) string {
// 		itemInt, _ := strconv.Atoi(item)
// 		return fmt.Sprintf("%d", itemInt%workersCount)
// 	}

// 	randomHash := func(workersCount int) string {
// 		return BROADCAST_ID
// 	}

// 	t.Run("TestDelta1Stage", func(t *testing.T) {
// 		REDUCE_EXCHANGE := testInfraConfig.GetReduceExchange()

// 		t.Run("Test Delta1Stage with one reducer", func(t *testing.T) {
// 			infra := model.NewInfraConfig(
// 				BROADCAST_ID,
// 				&model.WorkerClusterConfig{
// 					ReduceCount: 1,
// 				},
// 				testInfraConfig.GetRabbit(),
// 				"",
// 			)

// 			mapper := &Mapper{
// 				infraConfig:    infra,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			data := []*protocol.Delta_1_Data{
// 				{Country: "USA", Budget: 100},
// 				{Country: "USA", Budget: 200},
// 				{Country: "UK", Budget: 150},
// 			}

// 			tasks := mapper.delta1Stage(data, CLIENT_ID, CLIENT_ID, 0)

// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.DELTA_STAGE_2)
// 			assert.Len(t, tasks[REDUCE_EXCHANGE][c.DELTA_STAGE_2], 1) // ReduceCount is 1

// 			resultData := tasks[REDUCE_EXCHANGE][c.DELTA_STAGE_2][BROADCAST_ID].GetDelta_2().GetData()
// 			assert.Len(t, resultData, 2) // Should contain 2 countries
// 		})

// 		t.Run("Test Delta1Stage with multiple reducers", func(t *testing.T) {
// 			infra := model.NewInfraConfig(
// 				BROADCAST_ID,
// 				&model.WorkerClusterConfig{
// 					ReduceCount: 2,
// 				},
// 				testInfraConfig.GetRabbit(),
// 				"",
// 			)

// 			mapper := &Mapper{
// 				infraConfig:    infra,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			data := []*protocol.Delta_1_Data{
// 				{Country: "USA", Budget: 100},
// 				{Country: "USA", Budget: 200},
// 				{Country: "UK", Budget: 150},
// 			}

// 			tasks := mapper.delta1Stage(data, CLIENT_ID, CLIENT_ID, 0)

// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.DELTA_STAGE_2)

// 			if len(tasks[REDUCE_EXCHANGE][c.DELTA_STAGE_2]) > 2 || len(tasks[REDUCE_EXCHANGE][c.DELTA_STAGE_2]) == 0 {
// 				assert.Fail(t, "Because of random behaviour, it should be 1 or $ReduceCount tasks, got %v", len(tasks[REDUCE_EXCHANGE][c.DELTA_STAGE_2]))
// 			}
// 		})

// 		t.Run("Test Delta1Stage with empty data", func(t *testing.T) {
// 			infra := model.NewInfraConfig(
// 				BROADCAST_ID,
// 				&model.WorkerClusterConfig{
// 					ReduceCount: 2,
// 				},
// 				testInfraConfig.GetRabbit(),
// 				"",
// 			)

// 			mapper := &Mapper{
// 				infraConfig:    infra,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			data := []*protocol.Delta_1_Data{}

// 			tasks := mapper.delta1Stage(data, CLIENT_ID, CLIENT_ID, 0)

// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.DELTA_STAGE_2)
// 			assert.Len(t, tasks[REDUCE_EXCHANGE][c.DELTA_STAGE_2], 0) // Empty data should return no tasks
// 		})
// 	})

// 	t.Run("TestEta1Stage", func(t *testing.T) {
// 		REDUCE_EXCHANGE := testInfraConfig.GetReduceExchange()

// 		t.Run("Test Eta1Stage with one reducer", func(t *testing.T) {
// 			infra := model.NewInfraConfig(
// 				BROADCAST_ID,
// 				&model.WorkerClusterConfig{
// 					ReduceCount: 1,
// 				},
// 				testInfraConfig.GetRabbit(),
// 				"",
// 			)

// 			mapper := &Mapper{
// 				infraConfig:    infra,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			data := []*protocol.Eta_1_Data{
// 				{MovieId: "0", Title: "Movie 0", Rating: 4},
// 				{MovieId: "0", Title: "Movie 0", Rating: 5},
// 				{MovieId: "1", Title: "Movie 1", Rating: 3},
// 			}

// 			tasks := mapper.eta1Stage(data, CLIENT_ID, CLIENT_ID, 0)

// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.ETA_STAGE_2)
// 			assert.Len(t, tasks[REDUCE_EXCHANGE][c.ETA_STAGE_2], 1) // ReduceCount is 1
// 			resultData := tasks[REDUCE_EXCHANGE][c.ETA_STAGE_2][BROADCAST_ID].GetEta_2().GetData()
// 			assert.Len(t, resultData, 2) // Should contain 2 movies
// 		})

// 		t.Run("Test Eta1Stage with multiple reducers", func(t *testing.T) {
// 			mapper := &Mapper{
// 				infraConfig:    testInfraConfig,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			data := []*protocol.Eta_1_Data{
// 				{MovieId: "0", Title: "Movie 0", Rating: 4},
// 				{MovieId: "0", Title: "Movie 0", Rating: 5},
// 				{MovieId: "1", Title: "Movie 1", Rating: 3},
// 			}

// 			tasks := mapper.eta1Stage(data, CLIENT_ID, CLIENT_ID, 0)

// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.ETA_STAGE_2)
// 			assert.Len(t, tasks[REDUCE_EXCHANGE][c.ETA_STAGE_2], 1) // randomHash set to "0"

// 			result0Data := tasks[REDUCE_EXCHANGE][c.ETA_STAGE_2][BROADCAST_ID].GetEta_2().GetData()
// 			assert.Len(t, result0Data, 2) // Should contain 2 movies
// 		})

// 		t.Run("Test Eta1Stage with empty data", func(t *testing.T) {
// 			mapper := &Mapper{
// 				infraConfig:    testInfraConfig,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			data := []*protocol.Eta_1_Data{}

// 			tasks := mapper.eta1Stage(data, CLIENT_ID, CLIENT_ID, 0)

// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.ETA_STAGE_2)
// 			assert.Len(t, tasks[REDUCE_EXCHANGE][c.ETA_STAGE_2], 0) // Empty data should return no tasks
// 		})
// 	})

// 	t.Run("TestKappa1Stage", func(t *testing.T) {
// 		REDUCE_EXCHANGE := testInfraConfig.GetReduceExchange()

// 		t.Run("Test Kappa1Stage with one reducer", func(t *testing.T) {
// 			infra := model.NewInfraConfig(
// 				BROADCAST_ID,
// 				&model.WorkerClusterConfig{
// 					ReduceCount: 1,
// 				},
// 				testInfraConfig.GetRabbit(),
// 				"",
// 			)

// 			mapper := &Mapper{
// 				infraConfig:    infra,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			data := []*protocol.Kappa_1_Data{
// 				{ActorId: "0", ActorName: "Actor 0"},
// 				{ActorId: "0", ActorName: "Actor 0"},
// 				{ActorId: "1", ActorName: "Actor 1"},
// 			}

// 			tasks := mapper.kappa1Stage(data, CLIENT_ID, CLIENT_ID, 0)

// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.KAPPA_STAGE_2)
// 			assert.Len(t, tasks[REDUCE_EXCHANGE][c.KAPPA_STAGE_2], 1) // ReduceCount is 1
// 			resultData := tasks[REDUCE_EXCHANGE][c.KAPPA_STAGE_2][BROADCAST_ID].GetKappa_2().GetData()
// 			assert.Len(t, resultData, 2) // Should contain 2 actors

// 		})

// 		t.Run("Test Kappa1Stage with multiple reducers", func(t *testing.T) {
// 			mapper := &Mapper{
// 				infraConfig:    testInfraConfig,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			data := []*protocol.Kappa_1_Data{
// 				{ActorId: "0", ActorName: "Actor 0"},
// 				{ActorId: "0", ActorName: "Actor 0"},
// 				{ActorId: "1", ActorName: "Actor 1"},
// 			}

// 			tasks := mapper.kappa1Stage(data, CLIENT_ID, CLIENT_ID, 0)

// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.KAPPA_STAGE_2)
// 			result0Data := tasks[REDUCE_EXCHANGE][c.KAPPA_STAGE_2][BROADCAST_ID].GetKappa_2().GetData()
// 			assert.Len(t, result0Data, 2) // Should contain 2 actors
// 		})

// 		t.Run("Test Kappa1Stage with empty data", func(t *testing.T) {
// 			mapper := &Mapper{
// 				infraConfig:    testInfraConfig,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			data := []*protocol.Kappa_1_Data{}

// 			tasks := mapper.kappa1Stage(data, CLIENT_ID, CLIENT_ID, 0)

// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.KAPPA_STAGE_2)
// 			assert.Len(t, tasks[REDUCE_EXCHANGE][c.KAPPA_STAGE_2], 0) // Empty data should return no tasks
// 		})
// 	})

// 	t.Run("TestNu1Stage", func(t *testing.T) {
// 		REDUCE_EXCHANGE := testInfraConfig.GetReduceExchange()

// 		t.Run("Test Nu1Stage with one reducer", func(t *testing.T) {
// 			infra := model.NewInfraConfig(
// 				BROADCAST_ID,
// 				&model.WorkerClusterConfig{
// 					ReduceCount: 1,
// 				},
// 				testInfraConfig.GetRabbit(),
// 				"",
// 			)
// 			mapper := &Mapper{
// 				infraConfig:    infra,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			data := []*protocol.Nu_1_Data{
// 				{Id: "0", Sentiment: true, Revenue: 1000, Budget: 500},
// 				{Id: "0", Sentiment: false, Revenue: 2000, Budget: 1000},
// 				{Id: "1", Sentiment: true, Revenue: 1500, Budget: 750},
// 			}

// 			tasks := mapper.nu1Stage(data, CLIENT_ID, CLIENT_ID, 0)

// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.NU_STAGE_2)
// 			assert.Len(t, tasks[REDUCE_EXCHANGE][c.NU_STAGE_2], 1) // ReduceCount is 1
// 			resultData := tasks[REDUCE_EXCHANGE][BROADCAST_ID][0].GetNu_2().GetData()
// 			assert.Len(t, resultData, 2) // Should contain 2 movies
// 		})

// 		t.Run("Test Nu1Stage with multiple reducers", func(t *testing.T) {
// 			mapper := &Mapper{
// 				infraConfig:    testInfraConfig,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			data := []*protocol.Nu_1_Data{
// 				{Id: "0", Sentiment: true, Revenue: 1000, Budget: 500},
// 				{Id: "0", Sentiment: false, Revenue: 2000, Budget: 1000},
// 				{Id: "1", Sentiment: true, Revenue: 1500, Budget: 750},
// 			}

// 			tasks := mapper.nu1Stage(data, CLIENT_ID, CLIENT_ID, 0)

// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.NU_STAGE_2)
// 			assert.Len(t, tasks[REDUCE_EXCHANGE][BROADCAST_ID][0].GetNu_2().GetData(), 2)
// 		})

// 		t.Run("Test Nu1Stage with empty data", func(t *testing.T) {
// 			mapper := &Mapper{
// 				infraConfig:    testInfraConfig,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			data := []*protocol.Nu_1_Data{}

// 			tasks := mapper.nu1Stage(data, CLIENT_ID, CLIENT_ID, 0)

// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.NU_STAGE_2)
// 			assert.Len(t, tasks[REDUCE_EXCHANGE][c.NU_STAGE_2], 0) // Empty data should return no tasks
// 		})
// 	})

// 	t.Run("TestExecuteMapper", func(t *testing.T) {
// 		REDUCE_EXCHANGE := testInfraConfig.GetReduceExchange()

// 		t.Run("Test ExecuteMapper with Delta_1 stage", func(t *testing.T) {
// 			mapper := &Mapper{
// 				infraConfig:    testInfraConfig,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			task := &protocol.Task{
// 				Stage: &protocol.Task_Delta_1{
// 					Delta_1: &protocol.Delta_1{
// 						Data: []*protocol.Delta_1_Data{
// 							{Country: "USA", Budget: 100},
// 							{Country: "UK", Budget: 200},
// 						},
// 					},
// 				},
// 			}

// 			tasks, err := mapper.Execute(task)

// 			assert.NoError(t, err)
// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.DELTA_STAGE_2)
// 		})

// 		t.Run("Test ExecuteMapper with Eta_1 stage", func(t *testing.T) {
// 			mapper := &Mapper{
// 				infraConfig:    testInfraConfig,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			task := &protocol.Task{
// 				Stage: &protocol.Task_Eta_1{
// 					Eta_1: &protocol.Eta_1{
// 						Data: []*protocol.Eta_1_Data{
// 							{MovieId: "0", Title: "Movie 0", Rating: 4},
// 							{MovieId: "1", Title: "Movie 1", Rating: 5},
// 						},
// 					},
// 				},
// 			}

// 			tasks, err := mapper.Execute(task)

// 			assert.NoError(t, err)
// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.ETA_STAGE_2)
// 		})

// 		t.Run("Test ExecuteMapper with Kappa_1 stage", func(t *testing.T) {
// 			mapper := &Mapper{
// 				infraConfig:    testInfraConfig,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			task := &protocol.Task{
// 				Stage: &protocol.Task_Kappa_1{
// 					Kappa_1: &protocol.Kappa_1{
// 						Data: []*protocol.Kappa_1_Data{
// 							{ActorId: "0", ActorName: "Actor 0"},
// 							{ActorId: "1", ActorName: "Actor 1"},
// 						},
// 					},
// 				},
// 			}

// 			tasks, err := mapper.Execute(task)

// 			assert.NoError(t, err)
// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.KAPPA_STAGE_2)
// 		})

// 		t.Run("Test ExecuteMapper with Nu_1 stage", func(t *testing.T) {
// 			mapper := &Mapper{
// 				infraConfig:    testInfraConfig,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			task := &protocol.Task{
// 				Stage: &protocol.Task_Nu_1{
// 					Nu_1: &protocol.Nu_1{
// 						Data: []*protocol.Nu_1_Data{
// 							{Id: "0", Sentiment: true, Revenue: 1000, Budget: 500},
// 							{Id: "1", Sentiment: false, Revenue: 2000, Budget: 1000},
// 						},
// 					},
// 				},
// 			}

// 			tasks, err := mapper.Execute(task)

// 			assert.NoError(t, err)
// 			assert.Contains(t, tasks, REDUCE_EXCHANGE)
// 			assert.Contains(t, tasks[REDUCE_EXCHANGE], c.NU_STAGE_2)
// 		})

// 		t.Run("Test ExecuteMapper with unknown stage", func(t *testing.T) {
// 			mapper := &Mapper{
// 				infraConfig:    testInfraConfig,
// 				itemHashFunc:   itemHash,
// 				randomHashFunc: randomHash,
// 			}

// 			task := &protocol.Task{}

// 			tasks, err := mapper.Execute(task)

// 			assert.Error(t, err)
// 			assert.Nil(t, tasks)
// 		})
// 	})
// }
