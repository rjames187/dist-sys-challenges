package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var seen = []float64{}
var topology = map[string]any{}
var n = maelstrom.NewNode()

func broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
	}

	value := body["message"].(float64)

	// check if this node already received the broadcast
	for _, v := range seen {
		if value == v {
			return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
		}
	}

	seen = append(seen, value)

	for node, neighbors := range topology {
		if node == msg.Dest {
			for _, neighbor := range neighbors.([]any) {
				n.RPC(neighbor.(string), body, broadcastHandler)
			}
			break
		}
	}

	return n.Reply(msg, map[string]any{"type": "broadcast_ok"})
}

func main() {
	n.Handle("broadcast", broadcastHandler)

	n.Handle("read", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

    body["type"] = "read_ok"
		body["messages"] = seen

    return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

		topology = body["topology"].(map[string]any)

    body["type"] = "topology_ok"
		delete(body, "topology")

    return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
    log.Fatal(err)
	}
}