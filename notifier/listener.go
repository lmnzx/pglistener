package notifier

import (
	"context"
	"errors"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Listener interface {
	Close(ctx context.Context) error
	Connect(ctx context.Context) error
	Listen(ctx context.Context, topic string) error
	Ping(ctx context.Context) error
	Unlisten(ctx context.Context, topic string) error
	WaitForNotification(ctx context.Context) (*Notification, error)
}

func NewListener(dbp *pgxpool.Pool) Listener {
	return &listener{
		mu:     sync.Mutex{},
		dbPool: dbp,
	}
}

type listener struct {
	conn   *pgxpool.Conn
	dbPool *pgxpool.Pool
	mu     sync.Mutex
}

func (l *listener) Close(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Lock()

	if l.conn == nil {
		return nil
	}

	err := l.conn.Conn().Close(ctx)
	l.conn.Release()
	l.conn = nil
	return err
}

func (l *listener) Connect(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.conn != nil {
		return errors.New("connection already established")
	}

	conn, err := l.dbPool.Acquire(ctx)
	if err != nil {
		return err
	}

	l.conn = conn

	return nil
}

func (l *listener) Ping(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.conn.Ping(ctx)
}

func (l *listener) Listen(ctx context.Context, topic string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, err := l.conn.Exec(ctx, "LISTEN \""+topic+"\"")

	return err
}

func (l *listener) Unlisten(ctx context.Context, topic string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	_, err := l.conn.Exec(ctx, "UNLISTEN \""+topic+"\"")

	return err
}

func (l *listener) WaitForNotification(ctx context.Context) (*Notification, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	pgn, err := l.conn.Conn().WaitForNotification(ctx)

	if err != nil {
		return nil, err
	}

	n := Notification{
		Channel: pgn.Channel,
		Payload: []byte(pgn.Payload),
	}

	return &n, nil
}
