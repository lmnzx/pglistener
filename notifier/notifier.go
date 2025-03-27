package notifier

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"
)

type Notification struct {
	Channel string `json:"channel"`
	Payload []byte `json:"payload"`
}

type Subscription interface {
	NotificationC() <-chan []byte
	EstablishedC() <-chan struct{}
	Unlisten(ctx context.Context)
}

type Notifier interface {
	Listen(channel string) Subscription
	Run(ctx context.Context) error
}

type notifier struct {
	mu                        sync.RWMutex
	logger                    *slog.Logger
	listener                  Listener
	subscription              map[string][]*subscription
	channelChanges            []channelChange
	waitForNotificationCancel context.CancelFunc
}

type channelChange struct {
	channel   string
	close     func()
	operation string
}

type subscription struct {
	channel    string
	listenChan chan []byte
	notifier   *notifier

	establishedChan      chan struct{}
	establishedChanClose func()
	unlistenOnce         sync.Once
}

const listenerTimeout = 10 * time.Second

func (s *subscription) NotificationC() <-chan []byte { return s.listenChan }

func (s *subscription) EstablishedC() <-chan struct{} { return s.establishedChan }

func (s *subscription) Unlisten(ctx context.Context) {
	s.unlistenOnce.Do(func() {
		if err := s.notifier.unlistener(context.Background(), s); err != nil {
			s.notifier.logger.Error("error unlistening on channel", "err", err, "channel", s.channel)
		}
	})
}

func NewNotifier(l *slog.Logger, li Listener) Notifier {
	return &notifier{
		mu:                        sync.RWMutex{},
		logger:                    l,
		listener:                  li,
		subscription:              make(map[string][]*subscription),
		channelChanges:            []channelChange{},
		waitForNotificationCancel: context.CancelFunc(func() {}),
	}
}

func (n *notifier) Listen(channel string) Subscription {
	n.mu.Lock()
	defer n.mu.Unlock()

	existingSubs := n.subscription[channel]

	sub := &subscription{
		channel:    channel,
		listenChan: make(chan []byte, 2),
		notifier:   n,
	}
	n.subscription[channel] = append(n.subscription[channel], sub)

	if len(existingSubs) > 0 {
		sub.establishedChan = existingSubs[0].establishedChan
		sub.establishedChanClose = func() {}

		return sub
	}

	sub.establishedChan = make(chan struct{})
	sub.establishedChanClose = sync.OnceFunc(func() { close(sub.establishedChan) })

	n.channelChanges = append(n.channelChanges, channelChange{channel, sub.establishedChanClose, "listen"})
	n.waitForNotificationCancel()

	return sub
}

func (n *notifier) listenerListen(ctx context.Context, channel string) error {
	ctx, cancel := context.WithTimeout(ctx, listenerTimeout)
	defer cancel()

	n.logger.Debug("listening on channel", "channel", channel)
	if err := n.listener.Listen(ctx, channel); err != nil {
		return fmt.Errorf("error listening on channel %q: %w", channel, err)
	}

	return nil
}

func (n *notifier) listenerUnlisten(ctx context.Context, channel string) error {
	ctx, cancel := context.WithTimeout(ctx, listenerTimeout)
	defer cancel()

	n.logger.Debug("unlistening on channel", "channel", channel)
	if err := n.listener.Unlisten(ctx, channel); err != nil {
		return fmt.Errorf("error unlistening on channel %q: %w", channel, err)
	}

	return nil
}

func (n *notifier) unlistener(ctx context.Context, sub *subscription) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	subs := n.subscription[sub.channel]
	if len(subs) <= 1 {
		n.listenerUnlisten(ctx, sub.channel)
	}

	n.subscription[sub.channel] = slices.DeleteFunc(n.subscription[sub.channel], func(s *subscription) bool {
		return s == sub
	})
	if len(n.subscription[sub.channel]) < 1 {
		delete(n.subscription, sub.channel)
	}
	n.logger.Debug("removed subscription", "new_num_subscriptions", len(n.subscription[sub.channel]), "channel", sub.channel)

	return nil
}

func (n *notifier) processChannelChanges(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, u := range n.channelChanges {
		switch u.operation {
		case "listen":
			n.logger.Debug("listening to new channel", "channel", u.channel)
			n.listenerListen(ctx, u.channel)
			u.close()
		case "unlisten":
			n.logger.Debug("unlistening unlisten channel", "channel", u.channel)
			n.listenerUnlisten(ctx, u.channel)
		default:
			n.logger.Error("got unexpected change operation", "operation", u.operation)
		}
	}

	return nil
}

func (n *notifier) waitOnce(ctx context.Context) error {
	if err := n.processChannelChanges(ctx); err != nil {
		return err
	}

	notification, err := func() (*Notification, error) {
		const listenTimeout = 30 * time.Second

		ctx, cancel := context.WithTimeout(ctx, listenTimeout)
		defer cancel()

		n.mu.Lock()
		n.waitForNotificationCancel = cancel
		n.mu.Unlock()

		notification, err := n.listener.WaitForNotification(ctx)
		if err != nil {
			return nil, fmt.Errorf("error waiting for notication: %w", err)
		}
		return notification, nil
	}()

	if err != nil {
		if (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) && ctx.Err() == nil {
			return nil
		}
		return err
	}

	n.mu.RLock()
	defer n.mu.RUnlock()

	for _, sub := range n.subscription[notification.Channel] {
		select {
		case sub.listenChan <- []byte(notification.Payload):
		default:
			n.logger.Error("dropped notification due to full buffer", "payload", notification.Payload)
		}
	}

	return nil
}

func (n *notifier) Run(ctx context.Context) error {
	for {
		err := n.waitOnce(ctx)
		if err != nil || ctx.Err() != nil {
			return err
		}
	}
}
