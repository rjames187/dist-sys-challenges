package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type replica struct {
	dest string
	delta int
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()

	q := []replica{}
	mu := &sync.Mutex{}

	countStarted := false

	go func() {
		for {
			time.Sleep(time.Millisecond * 300)

			newQ := []replica{}
			wg := &sync.WaitGroup{}
			newQMu := &sync.Mutex{}

			mu.Lock()
			for _, data := range q {
				wg.Add(1)
				go func(rep replica) {
					defer wg.Done()
					_, err := n.SyncRPC(ctx, rep.dest, map[string]any{"type": "replicate", "delta": rep.delta})
					if err != nil {
						newQMu.Lock()
						newQ = append(newQ, rep)
						newQMu.Unlock()
					}
				}(data)
			}
			mu.Unlock()
			wg.Wait()
			q = newQ
		}
	}()

	n.Handle("add", func(msg maelstrom.Message) error {
		mu.Lock()
		if !countStarted {
			err := kv.CompareAndSwap(ctx, "count", 0, 0, true)
			if err != nil {
				return err
			}
			countStarted = true
		}
		mu.Unlock()


		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))
		
		mu.Lock()
		for _, node := range n.NodeIDs() {
			q = append(q, replica{node, delta})
		}
		mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("replicate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))
		count, err := kv.ReadInt(ctx, "count")
		if err != nil {
			return err
		}
		err = kv.Write(ctx, "count", delta + count)
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "replicate_ok"})
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