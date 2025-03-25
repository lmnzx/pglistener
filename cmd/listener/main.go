package main

import (
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	pglistener "pqlistener"
	"time"

	"github.com/lib/pq"
)

func main() {
	postgresDsn := os.Getenv("POSTGRES_URL")
	if postgresDsn == "" {
		log.Fatal("POSTGRES_URL environment variable not set")
	}

	const channel = "users_channel"

	listener := pq.NewListener(postgresDsn, time.Second*5, time.Minute, func(event pq.ListenerEventType, err error) {
		switch event {
		case pq.ListenerEventConnected:
			slog.Info("ListenerEventConnected")
		case pq.ListenerEventDisconnected:
			slog.Info("ListenerEventDisconnected")
		case pq.ListenerEventReconnected:
			slog.Info("ListenerEventReconnected")
		case pq.ListenerEventConnectionAttemptFailed:
			slog.Info("ListenerEventConnectionAttemptFailed")
		}
	})

	if err := listener.Listen(channel); err != nil {
		log.Fatal(err)
	}

	if err := listener.Ping(); err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case n := <-listener.Notify:
			var event pglistener.ChangeDataCaptureEvent[*pglistener.User]
			if err := json.Unmarshal([]byte(n.Extra), &event); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("%s - %s -%s\n", event.Table, event.Action, event.Data.Id)
		}
	}
}
