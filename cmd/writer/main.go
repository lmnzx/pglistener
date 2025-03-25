package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	pglistener "pqlistener"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
)

func main() {
	ctx := context.Background()
	postgresDsn := os.Getenv("POSTGRES_URL")
	if postgresDsn == "" {
		log.Fatal("POSTGRES_URL environment variable not set")
	}

	db, err := pglistener.NewPostgresDb(postgresDsn)
	if err != nil {
		log.Fatal(err)
	}

	store := pglistener.NewUserStore(db)
	// u, err := store.Create(ctx, pglistener.CreateUserParams{
	// 	FirstName: gofakeit.FirstName(),
	// 	LastName:  gofakeit.LastName(),
	// 	Email:     gofakeit.Email(),
	// })

	id := uuid.MustParse("011bf3b5-7496-4e46-a93a-339781b12a94")
	u, err := store.ById(ctx, id)
	if err != nil {
		log.Fatal(err)
	}

	// update
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			u.Email = gofakeit.Email()
			u, err := store.Update(ctx, u)
			if err != nil {
				log.Fatal(err)
			}
			b, err := json.MarshalIndent(u, "", "\t")
			if err != nil {
				log.Fatal(err)
			}

			fmt.Println(string(b))
		}
	}

}
