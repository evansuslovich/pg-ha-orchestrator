package main

import (
	"fmt"

	"github.com/evansuslovich/pg-ha-orchestrator/models"
)

func main() {
	fmt.Println("====   Hop on the Raft!  ==== \n")

	raft := models.NewRaft()
	raft.View()

	raft.Run()

}
