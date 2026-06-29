package main

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

type DeviceStatus struct {
	DeviceID  string `json:"device_id"`
	Timestamp int64  `json:"timestamp"`
	Version   string `json:"version"`
}

func main() {
	deviceID := os.Getenv("DEVICE_ID")
	natsURL := os.Getenv("NATS_URL")

	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("failed to get JetStream context: %v", err)
	}

	//! PRECREATED BUCKET
	kv, err := js.KeyValue("device-health")
	if err != nil {
		log.Fatalf("failed to get KV store: %v", err)
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		status := DeviceStatus{
			DeviceID:  deviceID,
			Timestamp: time.Now().Unix(),
			Version:   "1.0.0",
		}
		data, _ := json.Marshal(status)

		//! Put resets the TTL on the key
		rev, err := kv.Put(deviceID, data)
		if err != nil {
			log.Printf("failed to write heartbeat: %v", err)
			continue
		}
		log.Printf("heartbeat sent (rev %d)", rev)
	}
}
