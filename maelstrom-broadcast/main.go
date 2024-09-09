package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)


func main() {
	n := maelstrom.NewNode()

	state := map[float64]bool{}
	mu := &sync.Mutex{}
	heartbeatInterval := 400 * time.Millisecond
	done := false
	defer func() { done = true }()

	// background goroutine periodically sends a heartbeat with state info attached
	go func() {
		for {
			if done {
				break
			}
			time.Sleep(heartbeatInterval)

			stateToSend := []float64{}
			mu.Lock()
			for val := range state {
				stateToSend = append(stateToSend, val)
			}
			mu.Unlock()

			for _, node := range n.NodeIDs() {
				err := n.Send(node, map[string]any{"type": "heartbeat", "state": stateToSend})
				if err != nil {
					panic(err)
				}
			}
		}
	}()
	
	n.Handle("heartbeat", func(msg maelstrom.Message) error {
		var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

		mu.Lock()
		for _, val := range body["state"].([]any) {
			state[val.(float64)] = true
		}
		mu.Unlock()

		return nil
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }
		val := body["message"].(float64)
		mu.Lock()
		state[val] = true
		mu.Unlock()

		return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

		read := []float64{}
		mu.Lock()
		for val := range state {
			read = append(read, val)
		}
		mu.Unlock()

    return n.Reply(msg, map[string]any{"type": "read_ok", "messages": read})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

    body["type"] = "topology_ok"
		delete(body, "topology")

    return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
    log.Fatal(err)
	}
}