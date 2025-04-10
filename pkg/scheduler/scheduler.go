package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	if r.Method != "POST" {
		http.Error(w, "Method not allowed ,only POST request", http.StatusMethodNotAllowed)
		return
	}
	var userRequest UserRequest
	if err := json.NewDecoder(r.Body).Decode(&userRequest); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	log.Printf("Following request received: %+v", userRequest)

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
	marshalData, err := json.Marshal(response)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to marshal response. Error: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	w.Write(marshalData)

}
func (s *SchedulerServer) handleSchedulerStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed ,only GET request", http.StatusMethodNotAllowed)
		return
	}

	taskId := r.URL.Query().Get("task_id")
	if taskId == " " {
		http.Error(w, "task id is missing", http.StatusBadRequest)
		return
	}

	var task Task
	query := `select & from tasks where id=$1`
	err := s.dbPool.QueryRow(context.Background(), query, taskId).Scan(&task.ID, &task.Command, &task.ScheduledAt, &task.PickedAt, &task.StartedAt, &task.CompletedAt, &task.FailedAt)

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

	marshalResp, err := json.Marshal(response)
	if err != nil {
		http.Error(w, "Failed to marshal the response object", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(marshalResp)
}
func (s *SchedulerServer) insertTaskToDB(ctx context.Context, task Task) (string, error) {
	query := `insert into tasks (command , scheduled_at) values($1,$2) returning id`
	var insertedId string

	err := s.dbPool.QueryRow(ctx, query, task.Command, task.ScheduledAt).Scan(&insertedId)
	if err != nil {
		return "", err
	}
	return insertedId, nil
}
func (s *SchedulerServer) awaitShutDown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	<-stop

	return s.Stop()

}
func (s *SchedulerServer) Stop() error {
	defer s.dbPool.Close()

	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		return s.httpServer.Shutdown(ctx)
	}
	log.Println("Scheduler service closing")
	return nil
}
