package coordinator

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
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
	WorkerPool          map[uint32]*WorkerInfo
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

func (c *CoordinatorServer) Start() error {
	var err error

	// start WorkerPool
	// Workers can self-register just by sending a heartbeat. The server stays updated on which workers are alive.
	//Supports automatic recovery if a worker restarts and reconnects.

	go c.manageWorkerPool() // removes the inactive workers from the pool

	// start the grpcServer
	if err = c.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server error,coudn't start the server %w", err)
	}
	// connect to dbPool
	s.dbPool, err = common.ConnectToDB(c.ctx, c.dbString)
	if err != nil {
		return err
	}

	// scan the db at regular interval
	go c.scanDB()
	// returnt the await shutdown function if the program stops this also
	return c.awaitShutDown()
}

func (c *CoordinatorServer) awaitShutDown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop

	return c.Stop()
}

func (c *CoordinatorServer) Stop() error {}

// startGRPCServer function starts the gRPC server

func (c *CoordinatorServer) startGRPCServer() error {
	var err error
	c.listner, err = net.Listen("tcp", c.serverPort)
	if err != nil {
		return err
	}

	slog.Info("starting gRPC server", fmt.Sprintf("Addr"), c.serverPort)
	c.grpcServer = grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(c.grpcServer, c)

	go func() {
		if err := c.grpcServer.Serve(c.listner); err != nil {
			log.Fatal("gRPC server failed to start: %v", err)
		}
	}()

	return nil

}

// SubmitTask takes in the task from porotobuf defined spec and calls the submittaskWorker()
func (c *CoordinatorServer) SubmitTask(ctx context.Context, in *pb.ClientTaskRequest) (*pb.ClientTaskResponse, error) {
	// get the data from ClientTaskRequest
	data := in.GetClientData()

	// Attaches the uuid to each new task randomly
	taskID := uuid.New().String()

	// construct the task struct to send the response to client
	task := &pb.TaskRequest{
		TaskId: taskID,
		Data:   data,
	}
	if err := c.dispatchTasktoWorker(task); err != nil {
		return nil, err
	}

	return &pb.ClientTaskResponse{
		Message: "Task submitted successfully",
		TaskId:  taskID,
	}, nil

}
func (c *CoordinatorServer) dispatchTasktoWorker(task *pb.TaskRequest) error {}
