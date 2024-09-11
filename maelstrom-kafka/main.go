package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Log struct {
	data []float64
	mu *sync.Mutex
	cmtOffset int
}

func NewLog() *Log {
	return &Log{
		[]float64{},
		&sync.Mutex{},
		-1,
	}
}

func (l *Log) append(val float64) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.data = append(l.data, val)
	return len(l.data) - 1
}

type Broker struct {
	logs map[string]*Log
	mu *sync.Mutex
}

func NewBroker() *Broker {
	return &Broker{
		map[string]*Log{},
		&sync.Mutex{},
	}
}

func (b *Broker) getLog(key string) *Log {
	b.mu.Lock()
	defer b.mu.Unlock()

	aLog, ok := b.logs[key]
	if !ok {
		b.logs[key] = NewLog()
		aLog = b.logs[key]
	}
	return aLog
}

func main() {
	n := maelstrom.NewNode()
	broker := NewBroker()

	n.Handle("send", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

		key := body["key"].(string)
		val := body["msg"].(float64)

    aLog := broker.getLog(key)
		offset := aLog.append(val)

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
			idx := int(offset.(float64))
			if idx >= len(aLog.data) {
				continue
			}
			val := int(aLog.data[idx])

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