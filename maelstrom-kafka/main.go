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
	data []float64
	mu *sync.Mutex
	cmtOffset int
	kv *maelstrom.KV
	key string
}

func NewLog(kv *maelstrom.KV, key string) *Log {
	return &Log{
		[]float64{},
		&sync.Mutex{},
		-1,
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
	l.mu.Lock()
	defer l.mu.Unlock()

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
	b.mu.Lock()
	defer b.mu.Unlock()

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
			aLog.cmtOffset = int(offset.(float64))
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
			offsets[key.(string)] = aLog.cmtOffset
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