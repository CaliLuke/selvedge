package storage

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
)

type SignalRef struct {
	ID      int64
	Channel string
}
type SignalPosition struct {
	ID                        int64
	Channel                   string
	DeliveredSeq, ConsumedSeq int64
}
type DeliveryBatch struct {
	HistoryKeys []string
	Signals     []SignalRef
}
type HistoryFill struct {
	Key, Command string
	EventType    HistoryEventType
	EventData    []byte
}
type SignalConsume struct {
	ID                     int64
	Channel                string
	RequireDeliveredBefore int64
}
type TimerRetire struct{ Key string }
type ObservationCommit struct {
	Seq        int64
	Batch      *DeliveryBatch
	DeliverAll bool
	Record     *HistoryEvent
	Fill       *HistoryFill
	Consume    *SignalConsume
	Retire     *TimerRetire
}
type ObservationResult struct {
	Stored    *HistoryEvent
	Delivered DeliveryBatch
}
type DeliveryManager interface {
	ArrivedUndelivered(context.Context, string) (*DeliveryBatch, error)
	DeliveredSignals(context.Context, string) ([]SignalPosition, error)
	CommitObservation(context.Context, *Fence, *ObservationCommit) (*ObservationResult, error)
}

var ErrDeliveryConflict = errors.New("delivery candidate is absent or already delivered")

func readDelivery(ctx context.Context, conn Executor, id, condition string, args ...any) (*DeliveryBatch, error) {
	b := &DeliveryBatch{}
	params := append([]any{id}, args...)
	rows, err := conn.QueryContext(ctx, `SELECT activity_id FROM workflow_history WHERE instance_id=$1 AND event_data IS NOT NULL AND seq IS NOT NULL AND `+condition+` ORDER BY seq`, params...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			break
		}
		b.HistoryKeys = append(b.HistoryKeys, key)
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	if err != nil {
		return nil, err
	}
	rows, err = conn.QueryContext(ctx, `SELECT id,channel FROM workflow_signals WHERE instance_id=$1 AND `+condition+` ORDER BY id`, params...)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var ref SignalRef
		if err = rows.Scan(&ref.ID, &ref.Channel); err != nil {
			break
		}
		b.Signals = append(b.Signals, ref)
	}
	err = errors.Join(err, rows.Err(), rows.Close())
	return b, err
}
func deliveredSignals(ctx context.Context, conn Executor, id string) ([]SignalPosition, error) {
	rows, err := conn.QueryContext(ctx, `SELECT id,channel,delivered_seq,COALESCE(consumed_seq,0) FROM workflow_signals WHERE instance_id=$1 AND delivered_seq IS NOT NULL ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []SignalPosition
	for rows.Next() {
		var s SignalPosition
		if err := rows.Scan(&s.ID, &s.Channel, &s.DeliveredSeq, &s.ConsumedSeq); err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}
func (s signalStore) commitObservation(ctx context.Context, f *Fence, c *ObservationCommit) (*ObservationResult, error) {
	if f == nil {
		return nil, rejectFence(ctx)
	}
	if c == nil || c.Seq <= 0 || (c.Record == nil) == (c.Fill == nil) || (c.DeliverAll && c.Batch != nil) || c.Retire != nil {
		return nil, errors.New("invalid observation commit")
	}
	key, command := "", ""
	if c.Record != nil {
		if c.Record.InstanceID != f.InstanceID {
			return nil, rejectFence(ctx)
		}
		if c.Record.ActivityID == "" || c.Record.Command == "" || c.Record.Seq != c.Seq {
			return nil, ErrHistoryKeyConflict
		}
		key, command = c.Record.ActivityID, c.Record.Command
	} else {
		key, command = c.Fill.Key, c.Fill.Command
	}
	if c.Consume != nil && (c.Record == nil || c.Record.EventType != HistorySignalReceived || (command != "receive:"+c.Consume.Channel && command != "receive_async:"+c.Consume.Channel && command != "receive_timeout:"+c.Consume.Channel)) {
		return nil, ErrSignalNotAvailable
	}
	result := &ObservationResult{}
	err := s.transaction(ctx, func(ctx context.Context, conn Executor) error {
		return (fenceStore{signalTransaction{s.Storage, conn}}).withFence(ctx, f, func(conn Executor) error {
			previous, err := readHistory(ctx, conn, f.InstanceID, key)
			if err != nil {
				return err
			}
			if previous != nil && (previous.Command != command || previous.Seq != c.Seq) {
				return ErrHistoryKeyConflict
			}
			repeat := previous != nil && ((c.Record != nil && (previous.EventData != nil || previous.EventType == HistorySignalNone)) || (c.Fill != nil && previous.DeliveredSeq == c.Seq))
			if repeat {
				if c.Consume != nil {
					var id int64
					err := conn.QueryRowContext(ctx, `SELECT id FROM workflow_signals WHERE instance_id=$1 AND consumed_key=$2 AND channel=$3`, f.InstanceID, key, c.Consume.Channel).Scan(&id)
					if errors.Is(err, sql.ErrNoRows) {
						return ErrHistoryKeyConflict
					}
					if err != nil {
						return err
					}
					if id != c.Consume.ID {
						return ErrSignalNotAvailable
					}
				}
				b, err := readDelivery(ctx, conn, f.InstanceID, "delivered_seq=$2", c.Seq)
				if err != nil {
					return err
				}
				result.Stored = previous
				result.Delivered = *b
				return nil
			}
			if c.Record != nil && previous != nil {
				return ErrHistoryKeyConflict
			}
			if c.Fill != nil {
				result.Stored, err = writeHistory(ctx, conn, &HistoryEvent{InstanceID: f.InstanceID, ActivityID: key, Command: command, EventType: c.Fill.EventType, EventData: c.Fill.EventData}, true)
				if err != nil {
					return err
				}
			}
			batch := c.Batch
			if c.DeliverAll {
				batch, err = readDelivery(ctx, conn, f.InstanceID, "delivered_seq IS NULL")
				if err != nil {
					return err
				}
			}
			if batch != nil {
				if err := tagDelivery(ctx, conn, f.InstanceID, c.Seq, batch); err != nil {
					return err
				}
			}
			if c.Record != nil {
				obs := *c.Record
				if c.Consume != nil {
					payload, err := consumeAt(ctx, conn, f.InstanceID, c.Seq, key, c.Consume)
					if err != nil {
						return err
					}
					if obs.EventData != nil && !bytes.Equal(obs.EventData, payload) {
						return errors.New("signal observation payload does not match inbox payload")
					}
					obs.EventData = payload
				}
				result.Stored, err = writeHistory(ctx, conn, &obs, false)
			} else {
				result.Stored, err = readHistory(ctx, conn, f.InstanceID, key)
			}
			if err != nil {
				return err
			}
			b, err := readDelivery(ctx, conn, f.InstanceID, "delivered_seq=$2", c.Seq)
			if err != nil {
				return err
			}
			result.Delivered = *b
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}
func tagDelivery(ctx context.Context, conn Executor, id string, seq int64, b *DeliveryBatch) error {
	for _, key := range b.HistoryKeys {
		var stored string
		err := conn.QueryRowContext(ctx, `UPDATE workflow_history SET delivered_seq=$1 WHERE instance_id=$2 AND activity_id=$3 AND delivered_seq IS NULL AND event_data IS NOT NULL RETURNING activity_id`, seq, id, key).Scan(&stored)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrDeliveryConflict
		}
		if err != nil {
			return err
		}
	}
	for _, ref := range b.Signals {
		var stored int64
		err := conn.QueryRowContext(ctx, `UPDATE workflow_signals SET delivered_seq=$1 WHERE instance_id=$2 AND id=$3 AND channel=$4 AND delivered_seq IS NULL RETURNING id`, seq, id, ref.ID, ref.Channel).Scan(&stored)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrDeliveryConflict
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func consumeAt(ctx context.Context, conn Executor, id string, seq int64, key string, c *SignalConsume) ([]byte, error) {
	var payload string
	err := conn.QueryRowContext(ctx, `UPDATE workflow_signals SET consumed_key=$1,consumed_seq=$2,delivered_seq=COALESCE(delivered_seq,$2)
 WHERE instance_id=$3 AND id=$4 AND channel=$5 AND consumed_key IS NULL AND (CAST($6 AS BIGINT)=0 OR delivered_seq <= $6) RETURNING payload`, key, seq, id, c.ID, c.Channel, c.RequireDeliveredBefore).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrSignalNotAvailable
	}
	return []byte(payload), err
}
func (s *SQLiteStorage) ArrivedUndelivered(ctx context.Context, id string) (*DeliveryBatch, error) {
	return readDelivery(ctx, s.Conn(ctx), id, "delivered_seq IS NULL")
}
func (s *PostgresStorage) ArrivedUndelivered(ctx context.Context, id string) (*DeliveryBatch, error) {
	return readDelivery(ctx, s.Conn(ctx), id, "delivered_seq IS NULL")
}
func (s *SQLiteStorage) DeliveredSignals(ctx context.Context, id string) ([]SignalPosition, error) {
	return deliveredSignals(ctx, s.Conn(ctx), id)
}
func (s *PostgresStorage) DeliveredSignals(ctx context.Context, id string) ([]SignalPosition, error) {
	return deliveredSignals(ctx, s.Conn(ctx), id)
}
func (s *SQLiteStorage) CommitObservation(ctx context.Context, f *Fence, c *ObservationCommit) (*ObservationResult, error) {
	return (signalStore{Storage: s, driver: s.driver}).commitObservation(ctx, f, c)
}
func (s *PostgresStorage) CommitObservation(ctx context.Context, f *Fence, c *ObservationCommit) (*ObservationResult, error) {
	return (signalStore{Storage: s, driver: s.driver, postgres: true}).commitObservation(ctx, f, c)
}
