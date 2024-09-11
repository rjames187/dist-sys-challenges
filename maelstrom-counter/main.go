package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()

	q := []int{}
	mu := &sync.Mutex{}

	startedCount := false

	go func() {
		for {
			time.Sleep(time.Millisecond * 300)

			mu.Lock()
			delta := 0
			for _, num := range q {
				delta += num
			}
			count, err := kv.ReadInt(ctx, "count")
			if err != nil {
				continue
			}
			err = kv.CompareAndSwap(ctx, "count", count, count + delta, true)
			if err != nil {
				continue
			}
			q = []int{}
			mu.Unlock()
		}
	}()

	n.Handle("add", func(msg maelstrom.Message) error {
		mu.Lock()
		if !startedCount {
			kv.CompareAndSwap(ctx, "count", 0, 0, true)
			startedCount = true
		}
		mu.Unlock()


		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))
		
		mu.Lock()
		q = append(q, delta)
		mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

    body["type"] = "read_ok"
		count, err := kv.ReadInt(ctx, "count")
		if err != nil {
			return err
		}
		body["value"] = count
    
    return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
    log.Fatal(err)
	}
}