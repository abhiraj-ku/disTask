package main

import (
	"flag"
	"log"

	"github.com/abhiraj-ku/disTask/pkg/coordinator"
)

var (
	coordPort = flag.String("coordinator_port", ":8084", "runs the corrdinator at this port")
)

func main() {
	dbConnection := common.GetDbConnString()
	cordServer := coordinator.NewServer(*coordPort, dbConnection)

	err := cordServer.Start()
	if err != nil {
		log.Fatal("Error starting the cord server: %v", err)
	}
}
