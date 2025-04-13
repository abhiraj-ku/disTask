package coordinator

import (
	"context"
	"errors"
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
	listner             net.Listener
	serverPort          string
	grpcServer          *grpc.Server
	WorkerPool          map[uint32]*WorkerInfo
	WorkerPoolKeys      []uint32
	WorkerPoolMutex     sync.Mutex
	WorkerPoolKeysMutex sync.RWMutex
	maxHeartBeatMisses  uint8
	heartBeatInterval   time.Duration
	roundRobinIndex     uint32
	dbString            string
	dbPool              *pgxpool.Pool
	cancel              context.CancelFunc
	ctx                 context.Context
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

func (c *CoordinatorServer) Stop() error {
	//
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()

	// waits for anygroutine who is working once done lock the resources and then close each worker

	c.WorkerPoolMutex.Lock()
	defer c.WorkerPoolMutex.Unlock()
	for _, worker := range c.WorkerPool {
		if worker.grpcConnection != nil {
			worker.grpcConnection.Close()
		}
	}
	if c.grpcServer != nil {
		c.grpcServer.GracefulStop()
	}

	if c.listner != nil {
		return c.listner.Close()
	}

	if c.dbPool != nil {
		c.dbPool.Close()
	}
	return nil
}

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
func (c *CoordinatorServer) dispatchTasktoWorker(task *pb.TaskRequest) error {
	worker := c.getNextWorker()
	if worker == nil {
		return errors.New("no worker available to work")
	}

	_, err := worker.workerServiceClient.SubmitTask(context.Background(), task)
	return err

}
func (c *CoordinatorServer) getNextWorker() *WorkerInfo {

	// we will select the workers in a round robin kind of format
	// round robin --> 1->2->3->4 and so on , so we need to have lock on WorkerPool
	// to avoid deadlock
	c.WorkerPoolKeysMutex.RLock() // this method make sure the worker pool is being read only .
	defer c.WorkerPoolKeysMutex.RUnlock()

	workerCount := len(c.WorkerPoolKeys)
	if workerCount == 0 {
		return nil
	}

	/*
			 Round Robin visualization

			 CoordinatorServer.WorkerPoolKeys: ["worker-a", "worker-b", "worker-c"]
			 CoordinatorServer.WorkerPool:  this is dummy data and does not resembles the actual workerInfo struct see the workerInfo struct
			{
		    "worker-a": &workerInfo{ID: "worker-a", Address: "192.168.1.10:8080"},
		    "worker-b": &workerInfo{ID: "worker-b", Address: "192.168.1.11:8080"},
		    "worker-c": &workerInfo{ID: "worker-c", Address: "192.168.1.12:8080"},
			}

			initally roundRobinIndex == 0
			0 % 3 (len currently) == 0 ; WorkerPoolKeys[0] == "worker-a"
			WorkerPool["worker-a"] -> return the data --> "worker-a": &workerInfo{ID: "worker-a", Address: "192.168.1.10:8080"},

			next roundRobinIndex == 1 and this process continues
	*/

	nextWorker := c.WorkerPool[c.WorkerPoolKeys[c.roundRobinIndex%uint32(workerCount)]]
	c.roundRobinIndex++

	return nextWorker

}

// Function to scan db every time interval using time.Ticker

func (c *CoordinatorServer) scanDB() {
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			go c.ExecuteAllScheduledTask()
		case <-c.ctx.Done():
			log.Println("Shutting the db scanner method")
			return
		}
	}
}

// ExecuteAllScheduledTask fetches and delegates to workers
