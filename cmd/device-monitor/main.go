package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const bucketName = "device-health"

type DeviceStatus struct {
	Healthy bool   `json:"healthy"`
	Version string `json:"version"`
}

func main() {
	mu := &sync.RWMutex{}
	devices := map[string]*DeviceStatus{
		"device-1": {
			Healthy: true,
			Version: "1.0.0",
		},
		"device-2": {
			Healthy: true,
			Version: "1.0.0",
		},
	}

	go func() {
		for {
			mu.Lock()
			for id, deviceStatus := range devices {
				fmt.Printf("*** Device %s ***\n%v\n", id, *deviceStatus)
			}
			mu.Unlock()
			time.Sleep(10 * time.Second)
		}
	}()

	nc, err := nats.Connect(os.Getenv("NATS_URL"))
	if err != nil {
		log.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()

	// Initialize the new JetStream Simplified Client
	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	ctx := context.Background()

	// Bind to the existing KeyValue store
	kv, err := js.KeyValue(ctx, bucketName)
	if err != nil {
		log.Fatalf("kv store: %v", err)
	}

	// Instantiate the watcher to listen for changes in the KeyValue store
	watcher, err := kv.WatchAll(ctx)
	if err != nil {
		log.Fatalf("kv watch: %v", err)
	}
	defer watcher.Stop()

	//! Wait 3 seconds and SYNC!
	time.Sleep(3 * time.Second)

	for id, device := range devices {
		_, err := kv.Get(ctx, id)
		if err != nil {
			if errors.Is(err, jetstream.ErrKeyNotFound) {
				log.Printf("key not found. setting device %s as %t\n", id, false)
				mu.Lock()
				device.Healthy = false
				mu.Unlock()
				continue
			}

			log.Printf("failed to get device %s status: %v", id, err)
			continue
		}

		log.Printf("found. setting device %s as %t\n", id, true)
		mu.Lock()
		device.Healthy = true
		mu.Unlock()
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	log.Println("device-monitor running…")

	for {
		select {
		case entry, ok := <-watcher.Updates():
			if !ok {
				return
			}
			if entry == nil {
				continue
			}

			deviceID := entry.Key()

			switch entry.Operation() {
			case jetstream.KeyValuePut:
				log.Printf("device %s isHealthy=true content=%s", deviceID, entry.Value())
				mu.Lock()
				devices[deviceID].Healthy = true
				mu.Unlock()

			case jetstream.KeyValueDelete:
				log.Printf("device %s isHealthy=false (DELETED)", deviceID)
				mu.Lock()
				devices[deviceID].Healthy = false
				mu.Unlock()

			case jetstream.KeyValuePurge:
				log.Printf("device %s isHealthy=false (PURGED/TTL EXPIRED)", deviceID)
				mu.Lock()
				devices[deviceID].Healthy = false
				mu.Unlock()

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
