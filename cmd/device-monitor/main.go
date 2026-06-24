package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

const (
	deviceGroup    = "devices.example.com"
	deviceVersion  = "v1"
	deviceResource = "devices"
	bucketName     = "device-health"
)

type Condition struct {
	Type               string      `json:"type"`
	Status             string      `json:"status"`
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`
	Reason             string      `json:"reason,omitempty"`
	Message            string      `json:"message,omitempty"`
}

func main() {
	// // --- Kubernetes client (dynamic, to work with any CRD) ---
	// kubeconfig := os.Getenv("KUBECONFIG")
	// config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	// if err != nil {
	// 	log.Fatalf("failed to build kubeconfig: %v", err)
	// }
	// dynClient, err := dynamic.NewForConfig(config)
	// if err != nil {
	// 	log.Fatalf("failed to create dynamic client: %v", err)
	// }
	// deviceGVR := schema.GroupVersionResource{
	// 	Group:    deviceGroup,
	// 	Version:  deviceVersion,
	// 	Resource: deviceResource,
	// }

	nc, err := nats.Connect(os.Getenv("NATS_URL"))
	if err != nil {
		log.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		log.Fatalf("jetstream: %v", err)
	}

	kv, err := js.KeyValue(bucketName)
	if err != nil {
		log.Fatalf("kv store: %v", err)
	}

	// ctx := context.Background()
	// deviceList, err := dynClient.Resource(deviceGVR).List(ctx, metav1.ListOptions{})
	// if err != nil {
	// 	log.Fatalf("list devices: %v", err)
	// }

	// for _, item := range deviceList.Items {
	// 	deviceID := item.GetName()
	// 	_, err := kv.Get(deviceID)
	// 	isHealthy := (err == nil)
	// 	updateCondition(dynClient, deviceGVR, &item, isHealthy)
	// }

	watcher, err := kv.WatchAll()
	if err != nil {
		log.Fatalf("kv watch: %v", err)
	}
	defer watcher.Stop()

	// Devices expected to send heartbeats
	devices := map[string]bool{
		"device-123": true,
		"device-456": true,
		"device-789": true,
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Graceful shutdown
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

			switch entry.Operation() {
			case nats.KeyValuePut:
				log.Printf("device %s isHealthy=true", deviceID)

			default:
				log.Printf(
					"device %s watcher event operation=%v",
					deviceID,
					entry.Operation(),
				)
			}

		case <-ticker.C:
			checkDeviceHealth(kv, devices)

		case <-quit:
			log.Println("shutting down…")
			return
		}
	}
}

func checkDeviceHealth(
	kv nats.KeyValue,
	expectedDevices map[string]bool,
) {
	ctx := context.Background()

	keys, err := kv.Keys()
	if err != nil {
		if err == nats.ErrNoKeysFound {
			keys = []string{}
		} else {
			log.Printf("kv keys: %v", err)
			return
		}
	}

	healthyDevices := make(map[string]bool)

	for _, key := range keys {
		healthyDevices[key] = true
	}

	for deviceID := range expectedDevices {
		if healthyDevices[deviceID] {
			log.Printf(
				"device %s isHealthy=true",
				deviceID,
			)
			continue
		}

		log.Printf(
			"device %s isHealthy=false (heartbeat expired)",
			deviceID,
		)

		// cr, err := dynClient.Resource(deviceGVR).Get(context.Background(), deviceID, metav1.GetOptions{})
		// if err != nil {
		// 	log.Printf("failed to get CR %s: %v", deviceID, err)
		// 	continue
		// }
		// updateCondition(dynClient, deviceGVR, cr, isHealthy)
	}

	_ = ctx
}

// updateCondition sets the "Ready" condition to True/False and updates the
// resource status. It only writes if the condition actually changed.
func updateCondition(
	client dynamic.Interface,
	gvr schema.GroupVersionResource,
	obj *unstructured.Unstructured,
	isHealthy bool,
) {
	conditionsRaw, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil {
		log.Printf("error reading conditions for %s: %v", obj.GetName(), err)
		return
	}
	var conditions []Condition
	if found {
		data, _ := json.Marshal(conditionsRaw)
		json.Unmarshal(data, &conditions)
	}

	// Find or create the Ready condition
	readyIdx := -1
	for i, c := range conditions {
		if c.Type == "Ready" {
			readyIdx = i
			break
		}
	}
	if readyIdx == -1 {
		conditions = append(conditions, Condition{Type: "Ready"})
		readyIdx = len(conditions) - 1
	}

	targetStatus := "False"
	if isHealthy {
		targetStatus = "True"
	}

	// Only update if status changed
	if conditions[readyIdx].Status == targetStatus {
		return
	}

	now := metav1.Now()
	conditions[readyIdx].Status = targetStatus
	conditions[readyIdx].LastTransitionTime = now
	if targetStatus == "False" {
		conditions[readyIdx].Reason = "HeartbeatLost"
		conditions[readyIdx].Message = fmt.Sprintf("No heartbeat received for %s since %s",
			obj.GetName(), now.Format(time.RFC3339))
	} else {
		conditions[readyIdx].Reason = "HeartbeatReceived"
		conditions[readyIdx].Message = "Device is sending heartbeats"
	}

	// Convert back to unstructured and update the status subresource
	condIface := make([]interface{}, len(conditions))
	for i, c := range conditions {
		condIface[i] = map[string]interface{}{
			"type":               c.Type,
			"status":             c.Status,
			"lastTransitionTime": c.LastTransitionTime.Format(time.RFC3339),
			"reason":             c.Reason,
			"message":            c.Message,
		}
	}
	if err := unstructured.SetNestedSlice(obj.Object, condIface, "status", "conditions"); err != nil {
		log.Printf("failed to set conditions: %v", err)
		return
	}

	_, err = client.Resource(gvr).UpdateStatus(context.Background(), obj, metav1.UpdateOptions{})
	if err != nil {
		log.Printf("failed to update status for %s: %v", obj.GetName(), err)
	} else {
		log.Printf("updated %s ready=%s", obj.GetName(), targetStatus)
	}
}
