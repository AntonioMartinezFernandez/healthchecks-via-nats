package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream" // <-- Import the modern API
)

const bucketName = "device-health"

func main() {
	nc, err := nats.Connect(os.Getenv("NATS_URL"))
	if err != nil {
		log.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()

	// 1. Initialize the new JetStream Simplified Client
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	ctx := context.Background()

	// 2. Pass context to KeyValue
	kv, err := js.KeyValue(ctx, bucketName)
	if err != nil {
		log.Fatalf("kv store: %v", err)
	}

	// 3. Pass context to WatchAll
	watcher, err := kv.WatchAll(ctx)
	if err != nil {
		log.Fatalf("kv watch: %v", err)
	}
	defer watcher.Stop()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	log.Println("device-monitor running…")

	for {
		select {
		case entry := <-watcher.Updates():
			if entry == nil {
				continue
			}

			deviceID := entry.Key()

			// 4. Use jetstream constants for operations
			switch entry.Operation() {
			case jetstream.KeyValuePut:
				log.Printf("device %s isHealthy=true content=%s", deviceID, entry.Value())

			case jetstream.KeyValueDelete: // <-- Added Delete case
				log.Printf("device %s isHealthy=false (DELETED)", deviceID)

			case jetstream.KeyValuePurge:
				log.Printf("device %s isHealthy=false (PURGED/TTL EXPIRED)", deviceID)

			default:
				log.Printf(
					"device %s watcher event operation=%v",
					deviceID,
					entry.Operation(),
				)
			}

		case <-quit:
			log.Println("shutting down…")
			return
		}
	}
}
