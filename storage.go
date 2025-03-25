package pglistener

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type ChangeDataCaptureEvent[T any] struct {
	Table  string `json:"table"`
	Action string `json:"action"`
	Data   T      `json:"data"`
}

func NewPostgresDb(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres db connection: %v", err)
	}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to pong postgres db: %v", err)
	}

	return db, nil
}

type User struct {
	Id        uuid.UUID  `json:"id"`
	FirstName string     `json:"first_name"`
	LastName  string     `json:"last_name"`
	Email     string     `json:"email"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt *time.Time `json:"updated_at"`
}

type UserStore struct {
	db *sql.DB
}

func NewUserStore(db *sql.DB) *UserStore {
	return &UserStore{db}
}

type CreateUserParams struct {
	FirstName string
	LastName  string
	Email     string
}

func (s *UserStore) scan(row *sql.Row) (*User, error) {
	var user User
	if err := row.Scan(
		&user.Id,
		&user.FirstName,
		&user.LastName,
		&user.Email,
		&user.CreatedAt,
		&user.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("failed to scan user: %v", err)
	}
	return &user, nil
}

func (s *UserStore) Create(ctx context.Context, input CreateUserParams) (*User, error) {
	const insertQuery = `INSERT INTO users (first_name, last_name, email, updated_at)
                         VALUES($1, $2, $3, CURRENT_TIMESTAMP)
                         RETURNING *;`

	row := s.db.QueryRowContext(ctx, insertQuery, input.FirstName, input.LastName, input.Email)
	if row.Err() != nil {
		return nil, fmt.Errorf("failed to insert user: %v", row.Err())
	}
	return s.scan(row)
}

func (s *UserStore) Update(ctx context.Context, user *User) (*User, error) {
	const updateQuery = `UPDATE users 
                         SET first_name=$1, last_name=$2, email=$3, updated_at=CURRENT_TIMESTAMP
                         WHERE id=$4
                         RETURNING *;`

	row := s.db.QueryRowContext(ctx, updateQuery, user.FirstName, user.LastName, user.Email, user.Id)
	if row.Err() != nil {
		return nil, fmt.Errorf("failed to update user: %v", row.Err())
	}
	return s.scan(row)
}
