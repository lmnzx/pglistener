package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	pglistener "pqlistener"

	"github.com/google/uuid"
)

func main() {
	postgresDsn := os.Getenv("POSTGRES_URL")
	if postgresDsn == "" {
		log.Fatal("POSTGRES_URL environment variable not set")
	}

	const channel = "users_channel"

	listener, err := pglistener.NewPostgresListener(postgresDsn, channel)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case n := <-listener.Notify:
			var event pglistener.ChangeDataCaptureEvent[uuid.UUID, *pglistener.User]
			if err := json.Unmarshal([]byte(n.Extra), &event); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%s - %s -%s\n", event.Table, event.Action, event.Data.Id)
		}
	}
}
