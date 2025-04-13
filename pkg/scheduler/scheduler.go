package scheduler

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/abhiraj-ku/disTask/pkg/helper"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Command struct holds the structure in which client will
// send request
// command -> the actual command/task to execute
// scheduled_at -> when the user wants to execute

type UserRequest struct {
	Command     string `json:"command"`
	ScheduledAt string `json:"scheduled_at"`
}

// The actual task struct in accordance with the db schema for task
type Task struct {
	ID          string
	Command     string
	ScheduledAt pgtype.Timestamp
	PickedAt    pgtype.Timestamp
	StartedAt   pgtype.Timestamp
	CompletedAt pgtype.Timestamp
	FailedAt    pgtype.Timestamp
}

type SchedulerServer struct {
	port             string
	schConnectionURI string
	dbPool           *pgxpool.Pool
	ctx              context.Context
	cancel           context.CancelFunc
	httpServer       *http.Server
}

// it creates and returns the scheduler server with DI
func NewSchedulerServer(port string, connStr string) *SchedulerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &SchedulerServer{
		port:             port,
		schConnectionURI: connStr,
		ctx:              ctx,
		cancel:           cancel,
	}
}

func (s *SchedulerServer) Start() error {
	var err error

	// TODO: Implement the db connection with retry mechanism
	s.dbPool, err = common.ConnectDb(s.ctx, s.schConnectionURI)
	if err != nil {
		return err
	}

	http.HandleFunc("/schedule", s.handleScheduletask)
	http.HandleFunc("/status", s.handleSchedulerStatus)
	// start the server
	s.httpServer = &http.Server{
		Addr: s.port,
	}

	slog.Info("Starting scheduler server", fmt.Sprintf("Addr"), s.port)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error :%s\n", err)
		}
	}()

	// TODO: implement the gracefule shutdown function
	return s.awaitShutDown()

}

func (s *SchedulerServer) handleScheduletask(w http.ResponseWriter, r *http.Request) {

	// check if its post request or not
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed ,only POST request", http.StatusMethodNotAllowed)
		return
	}
	var userRequest UserRequest
	if err := helper.ReadJSON(w, r, &userRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	slog.Info("Request received", "request", userRequest)

	// Parse the time later convert to unix timestamp for storing in db
	scheduledTime, err := time.Parse(time.RFC3339, userRequest.ScheduledAt)
	if err != nil {
		http.Error(w, "Invalid time format,Only RFC3339 allowed", http.StatusBadRequest)
		return
	}

	convertToUnix := time.Unix(scheduledTime.Unix(), 0)

	// insert into db and get the task ID
	taskID, err := s.insertTaskToDB(context.Background(), Task{Command: userRequest.Command, ScheduledAt: pgtype.Timestamp{Time: convertToUnix}})

	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to submit task. Error: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	// Return above data
	response := struct {
		Command     string `json:"command"`
		ScheduledAt int64  `json:"scheduled_at"`
		TaskID      string `json:"task_id"`
	}{
		Command:     userRequest.Command,
		ScheduledAt: convertToUnix.Unix(),
		TaskID:      taskID,
	}
	err = helper.WriteJSON(w, http.StatusCreated, response)

}
func (s *SchedulerServer) handleSchedulerStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed ,only GET request", http.StatusMethodNotAllowed)
		return
	}

	taskId := r.URL.Query().Get("task_id")
	if taskId == "" {
		http.Error(w, "task id is required", http.StatusBadRequest)
		return
	}

	task, err := s.getTaskById(r.Context(), taskId)
	if err != nil {
		http.Error(w, "failed to fetch the task id ", http.StatusBadRequest)
		return
	}

	// Prepare the response JSON
	response := struct {
		TaskID      string `json:"task_id"`
		Command     string `json:"command"`
		ScheduledAt string `json:"scheduled_at,omitempty"`
		PickedAt    string `json:"picked_at,omitempty"`
		StartedAt   string `json:"started_at,omitempty"`
		CompletedAt string `json:"completed_at,omitempty"`
		FailedAt    string `json:"failed_at,omitempty"`
	}{
		TaskID:      task.ID,
		Command:     task.Command,
		ScheduledAt: "",
		PickedAt:    "",
		StartedAt:   "",
		CompletedAt: "",
		FailedAt:    "",
	}
	if task.ScheduledAt.Status == 2 {
		response.ScheduledAt = task.ScheduledAt.Time.String()
	}

	// Set the picked_at time if non-null.
	if task.PickedAt.Status == 2 {
		response.PickedAt = task.PickedAt.Time.String()
	}

	// Set the started_at time if non-null.
	if task.StartedAt.Status == 2 {
		response.StartedAt = task.StartedAt.Time.String()
	}

	// Set the completed_at time if non-null.
	if task.CompletedAt.Status == 2 {
		response.CompletedAt = task.CompletedAt.Time.String()
	}

	// Set the failed_at time if non-null.
	if task.FailedAt.Status == 2 {
		response.FailedAt = task.FailedAt.Time.String()
	}

	helper.WriteJSON(w, http.StatusCreated, response)
}

func (s *SchedulerServer) insertTaskToDB(ctx context.Context, task Task) (string, error) {
	query := `insert into tasks (command , scheduled_at) values($1,$2) returning id`
	var insertedId string

	err := s.dbPool.QueryRow(ctx, query, task.Command, task.ScheduledAt).Scan(&insertedId)
	if err != nil {
		return "", fmt.Errorf("database insertion error:%w", err)
	}
	return insertedId, nil
}

// getTaskById(ctx, taskID) (Task,error)
func (s *SchedulerServer) getTaskById(ctx context.Context, taskID string) (Task, error) {
	var task Task

	query := `select id, command, scheduled_at, picked_at, started_at, completed_at, failed_at 
	from tasks where id = $1`

	err := s.dbPool.QueryRow(ctx, query, taskID).Scan(
		&task.ID,
		&task.Command,
		&task.ScheduledAt,
		&task.PickedAt,
		&task.StartedAt,
		&task.CompletedAt,
		&task.FailedAt,
	)
	if err != nil {
		return Task{}, fmt.Errorf("error fetching task: %w", err)
	}
	return task, nil
}

func (s *SchedulerServer) awaitShutDown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case <-stop:
		slog.Info("shutdown signal recieved")
	case <-s.ctx.Done():
		slog.Info("context cancellsss")
	}

	return s.Stop()

}
func (s *SchedulerServer) Stop() error {
	slog.Info("Shutting down scheduler service")

	if s.cancel != nil {
		s.cancel()
	}

	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		if err := s.httpServer.Shutdown(ctx); err != nil {
			slog.Error("Server shutdown Error")
			return err
		}
	}

	if s.dbPool != nil {
		s.dbPool.Close()
	}
	log.Println("Scheduler service stopped")
	return nil
}
