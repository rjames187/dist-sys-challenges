package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var kvMu = &sync.Mutex{}

func read(nodeID string, kv *maelstrom.KV) (int, error) {
	ctx := context.TODO()

	count, err := kv.ReadInt(ctx, nodeID)
	rpcErr := &maelstrom.RPCError{}
	if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
		return 0, nil
	}
	return count, err
}

func writeDelta(delta int, nodeID string, kv *maelstrom.KV) error {
	ctx := context.TODO()

	kvMu.Lock()
	defer kvMu.Unlock()

	count, err := read(nodeID, kv)
	if err != nil {
		return err
	}

	err = kv.Write(ctx, nodeID, count + delta)

	return err
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	go func() {
		for {
			time.Sleep(600 * time.Millisecond)

			count, err := read(n.ID(), kv)
			if err != nil {
				panic(err)
			}
			for _, node := range n.NodeIDs() {
				if node != n.ID() {
					err = n.Send(node, map[string]any{"type": "heartbeat", "value": count})
					if err != nil {
						panic(err)
					}
				}
			}
		}
	}()
	
	n.Handle("heartbeat", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		value := int(body["value"].(float64))

		kvMu.Lock()
		defer kvMu.Unlock()
		count, err := read(msg.Src, kv)
		if err != nil {
			return err
		}
		ctx := context.TODO()
		if value > count {
			err = kv.Write(ctx, msg.Src, value)
			if err != nil {
				return err
			}
		}
		return nil
	})

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := int(body["delta"].(float64))
		
		err := writeDelta(delta, n.ID(), kv)
		if err != nil {
			return err
		}

		return n.Reply(msg, map[string]any{"type": "add_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

		total := 0
		for _, node := range n.NodeIDs() {
			count, err := read(node, kv)
			if err != nil {
				return err
			}
			total += count
		}
    
    return n.Reply(msg, map[string]any{"type": "read_ok", "value": total})
	})

	if err := n.Run(); err != nil {
    log.Fatal(err)
	}
}