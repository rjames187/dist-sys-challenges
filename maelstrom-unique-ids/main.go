package main

import (
	"encoding/json"
	"log"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func generateID(clientID string, messageID any) string {
	secondHalf := strconv.FormatFloat(messageID.(float64), 'f', -1, 64)
	return clientID + "x" + secondHalf
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
    // Unmarshal the message body as an loosely-typed map.
    var body map[string]any
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

    // Update the message type to return back.
    body["type"] = "generate_ok"

		// Generate the unique ID
		body["id"] = generateID(msg.Src, body["msg_id"])

    // Echo the original message back with the updated message type.
    return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
    log.Fatal(err)
	}
}