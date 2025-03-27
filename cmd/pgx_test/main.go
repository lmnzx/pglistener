package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	pglistener "pqlistener"
	"pqlistener/notifier"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()
	l := pglistener.GetDefaultLogger(slog.LevelInfo)

	const channel = "users_channel"

	postgresDsn := os.Getenv("POSTGRES_URL")
	if postgresDsn == "" {
		log.Fatal("POSTGRES_URL environment variable not set")
	}

	pool, err := pgxpool.New(ctx, postgresDsn)
	if err != nil {
		log.Fatalf("error connecting to db: %v", err)
	}

	if err = pool.Ping(ctx); err != nil {
		log.Fatalf("error pinging db: %v", err)
	}

	li := notifier.NewListener(pool)
	if err := li.Connect(ctx); err != nil {
		log.Fatalf("error setting up listener: %v", err)
	}

	n := notifier.NewNotifier(l, li)

	sub := n.Listen(channel)
	go n.Run(ctx)

	go func() {
		<-sub.EstablishedC()
		for {
			select {
			case <-ctx.Done():
				sub.Unlisten(ctx)
				return
			case p := <-sub.NotificationC():
				fmt.Println(string(p))
			}
		}
	}()

	select {}
}
