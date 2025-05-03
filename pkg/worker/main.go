package worker

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

const (
	taskProcessTime = 5 * time.Second
	workerPoolSize  = 5 * time.Second
)

// WorkerService represents a worker for a grpc based server
type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	id                       uint32
	serverPort               string
	coordinatorAddress       string
	listner                  net.Listener
	grpcServer               *grpc.Server
	coordinatorConnection    *grpc.ClientConn
	coordinatorServiceClient pb.coordinatorServiceClient
	heartbeatInterval        time.Duration
	taskQueue                chan *pb.TaskRequest
	RecievedTasks            map[string]*pb.TaskRequest
	ctx                      context.Context
	cancel                   context.CancelFunc
	wg                       sync.WaitGroup
}

// NewWorkerServer constructor creates and return WorkerServer

func NewWorkerServer(port string, coordinator string) *WorkerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerServer{
		id:                 uuid.New().ID(),
		serverPort:         port,
		coordinatorAddress: coordinator,
		heartbeatInterval:  common.DefaultHeartbeat,
		taskQueue:          make(chan *pb.TaskRequest, 100),
		RecievedTasks:      make(map[string]*pb.TaskRequest),
		ctx:                ctx,
		cancel:             cancel,
	}
}

// Start the worker service with new and restart also
func (w *WorkerServer) Start() error {
	w.startWorkerPool(workerPoolSize)

	if err := w.connectToCoordinator(); err != nil {
		return fmt.Errorf("failed to conncet to coordinator: %w", err)
	}
	defer w.closeGRPCConnection()

	go w.periodicHeartbeat()

	if err := w.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server start failed: %w", err)
	}

	return w.awaitShutdown()
}
