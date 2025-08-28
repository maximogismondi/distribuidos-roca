package common

// Query 1
const ALPHA_STAGE string = "alpha"
const BETA_STAGE string = "beta"

// Query 2
const GAMMA_STAGE string = "gamma"
const DELTA_STAGE_1 string = "delta_1"
const DELTA_STAGE_2 string = "delta_2"
const DELTA_STAGE_3 string = "delta_3"
const EPSILON_STAGE string = "epsilon"

// Query 3
const ZETA_STAGE string = "zeta"
const ETA_STAGE_1 string = "eta_1"
const ETA_STAGE_2 string = "eta_2"
const ETA_STAGE_3 string = "eta_3"
const THETA_STAGE string = "theta"

// Query 4
const IOTA_STAGE string = "iota"
const KAPPA_STAGE_1 string = "kappa_1"
const KAPPA_STAGE_2 string = "kappa_2"
const KAPPA_STAGE_3 string = "kappa_3"
const LAMBDA_STAGE string = "lambda"

// Query 5
const MU_STAGE string = "mu"
const NU_STAGE_1 string = "nu_1"
const NU_STAGE_2 string = "nu_2"
const NU_STAGE_3 string = "nu_3"

// EOF
const RING_STAGE string = "ringEOF"
const OMEGA_STAGE string = "omegaEOF"

// Consts for tests
const TEST_WORKER_COUNT int = 1
const TEST_WORKER_ID string = "0"

// TableType const
type TableType string

const (
	BIG_TABLE     TableType = "bigTable"
	SMALL_TABLE   TableType = "smallTable"
	GENERAL_TABLE TableType = "generalTable"
	NONE          TableType = ""
)

type FileName string

const (
	MIN     FileName = "_min"
	MAX     FileName = "_max"
	GENERAL FileName = ""
	// SMALL   FileName = "small"
	// BIG     FileName = "big"
)

type FolderType string

const (
	GENERAL_FOLDER_TYPE      FolderType = ""
	JOINER_SMALL_FOLDER_TYPE FolderType = "small"
	JOINER_BIG_FOLDER_TYPE   FolderType = "big"
)

type DataType string

// Heap const
const TYPE_MAX = "Max"
const TYPE_MIN = "Min"
