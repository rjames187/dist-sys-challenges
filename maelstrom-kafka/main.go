package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Log struct {
	mu *sync.Mutex
	kv *maelstrom.KV
	key string
}

func NewLog(kv *maelstrom.KV, key string) *Log {
	return &Log{
		&sync.Mutex{},
		kv,
		logKey(key),
	}
}

func (l *Log) retrieveLog() ([]any, error) {
	ctx := context.TODO()
	aLog, err := l.kv.Read(ctx, l.key)
	rpcErr := &maelstrom.RPCError{}
	if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
		return []any{}, nil
	}
	return aLog.([]any), err
}

func (l *Log) replaceLog(data []any) error {
	ctx := context.TODO()
	err := l.kv.Write(ctx, l.key, data)
	return err
}

func (l *Log) append(val float64) (int, error) {
	requestLock(l.key, l.kv)
	defer releaseLock(l.key, l.kv)

	data, err := l.retrieveLog()
	if err != nil {
		return 0, err
	}
	data = append(data, val)

	err = l.replaceLog(data); if err != nil {
		return 0, err
	}
	return len(data) - 1, nil
}

func (l *Log) commit(offset int) error {
	requestLock(l.key, l.kv)
	defer releaseLock(l.key, l.kv)
	ctx := context.TODO()

	err := l.kv.Write(ctx, commitKey(l.key), offset)
	return err
}

func (l *Log) getCommit() (int, error) {
	requestLock(l.key, l.kv)
	defer releaseLock(l.key, l.kv)
	ctx := context.TODO()

	offset, err := l.kv.ReadInt(ctx, commitKey(l.key))
	rpcErr := &maelstrom.RPCError{}
	if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
		return -1, nil
	}
	return offset, err
}

func requestLock(key string, kv *maelstrom.KV) {
	ctx := context.TODO()
	
	err := kv.CompareAndSwap(ctx, lockKey(key), 0, 1, true)
	rpcErr := &maelstrom.RPCError{}
	for errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.PreconditionFailed {
		err = kv.CompareAndSwap(ctx, "lock", 0, 1, true)
	}
}

func releaseLock(key string, kv *maelstrom.KV) error {
	ctx := context.TODO()

	err := kv.Write(ctx, lockKey(key), 0)
	return err
}

type Broker struct {
	logs map[string]*Log
	mu *sync.Mutex
	kv *maelstrom.KV
}

func NewBroker(kv *maelstrom.KV) *Broker {
	return &Broker{
		map[string]*Log{},
		&sync.Mutex{},
		kv,
	}
}

func (b *Broker) getLog(key string) *Log {
	requestLock("broker", b.kv)
	defer releaseLock("broker", b.kv)

	aLog, ok := b.logs[key]
	if !ok {
		b.logs[key] = NewLog(b.kv, key)
		aLog = b.logs[key]
	}
	return aLog
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	broker := NewBroker(kv)

	n.Handle("send", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

		key := body["key"].(string)
		val := body["msg"].(float64)

    aLog := broker.getLog(key)
		offset, err := aLog.append(val)
		if err != nil {
			return err
		}

    return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

		msgs := map[string][][]int{}

		offsets := body["offsets"].(map[string]any)
		for key, offset := range offsets {
			aLog := broker.getLog(key)
			data, err := aLog.retrieveLog()
			if err != nil {
				return err
			}

			idx := int(offset.(float64))
			if idx >= len(data) {
				continue
			}
			val := int(data[idx].(float64))

			if _, ok := msgs[key]; !ok {
				msgs[key] = [][]int{}
			}

			msgs[key] = append(msgs[key], []int{idx, val})
		}

		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": msgs})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

		offsets := body["offsets"].(map[string]any)
		for key, offset := range offsets {
			aLog := broker.getLog(key)
			err := aLog.commit(int(offset.(float64)))
			if err != nil {
				return err
			}
		}

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

		offsets := map[string]int{}
		keys := body["keys"].([]any)
		for _, key := range keys {
			aLog := broker.getLog(key.(string))
			offset, err := aLog.getCommit()
			if err != nil {
				return err
			}
			offsets[key.(string)] = offset
		}

		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": offsets})
	})

	if err := n.Run(); err != nil {
    log.Fatal(err)
	}
}

func logKey(key string) string {
	return fmt.Sprintf("log.%s", key)
}

func commitKey(key string) string {
	return fmt.Sprintf("cmt.%s", key)
}

func lockKey(key string) string {
	return fmt.Sprintf("lock.%s", key)
}