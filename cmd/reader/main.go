package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	pglistener "pqlistener"
	"syscall"
	"time"

	"github.com/google/uuid"
)

func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	postgresDsn := os.Getenv("POSTGRES_URL")
	if postgresDsn == "" {
		return fmt.Errorf("POSTGRES_URL environment variable not set")
	}

	const channel = "users_channel"
	listener, err := pglistener.NewPostgresListener(postgresDsn, channel)
	if err != nil {
		return fmt.Errorf("creating postgres listener: %v", err)
	}

	db, err := pglistener.NewPostgresDb(postgresDsn)
	if err != nil {
		return fmt.Errorf("creating postgres db: %v", err)
	}

	userStore := pglistener.NewUserStore(db)

	id := uuid.MustParse("011bf3b5-7496-4e46-a93a-339781b12a94")

	// ttl := time.Second * 15
	cache := pglistener.NewInMemoryCache(ctx, listener, userStore.ById)

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			user, err := cache.Get(ctx, id)
			if err != nil {
				return fmt.Errorf("getting user form cache: %v", err)
			}
			b, err := json.MarshalIndent(user, "", "\t")
			if err != nil {
				return fmt.Errorf("marshalling user: %v", err)
			}
			fmt.Println(string(b))
		}
	}

}

func main() {
	err := run()
	if err != nil {
		log.Fatal(err)
	}
}
