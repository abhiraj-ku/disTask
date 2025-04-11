package coordinator

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"google.golang.org/grpc"
)

// Declare some global constants like shutdowntime,default max miss ,
// and scanInterval for the coordinator to scan the db at this interval regularly
const (
	shutDowntime          = 7 * time.Second // Thala for a reason
	defaultMaxMissesforDb = 1
	scanInterval          = 7 * time.Second // Thala for a reason
)

type CoordinatorServer struct {
	serverPort          string
	listner             net.Listener
	grpcServer          *grpc.Server
	WorkerPool          map[uint32]*workerInfo
	WorkerPoolMutex     sync.Mutex
	WorkerPoolKeys      []uint32
	WorkerPoolKeysMutex sync.RWMutex
	maxHeartBeatMisses  uint8
	heartBeatInterval   time.Duration
	roundRobinIndex     uint32
	dbString            string
	dbPool              *pgxpool.Pool
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
}

type WorkerInfo struct {
	heartbeatMisses     uint8
	address             string
	grpcConnection      *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
}

// NewCoordinator server inits and return the server instance.
func NewServer(port string, dbconnStr string) *CoordinatorServer {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	return &CoordinatorServer{
		WorkerPool:         make(map[uint32]*WorkerInfo),
		maxHeartBeatMisses: defaultMaxMissesforDb,
		heartBeatInterval:  common.DefaultHeartbeat,
		dbString:           dbconnStr,
		serverPort:         port,
		ctx:                ctx,
		cancel:             cancel,
	}
}
