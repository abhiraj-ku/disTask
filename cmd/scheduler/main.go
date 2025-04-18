package main

import (
	"flag"
	"log"

	"github.com/abhiraj-ku/disTask/pkg/scheduler"
)

var (
	schedulerPort = flag.String("scheduler_port", ":8083", "scheduler server runs on this port")
)

func main() {
	dbConnection := common.GetDbConnString()
	scheduler := scheduler.NewSchedulerServer(*schedulerPort, dbConnection)

	err := scheduler.Start()
	if err != nil {
		log.Fatal("error starting the schdeuler server: %v", err)
	}
}
