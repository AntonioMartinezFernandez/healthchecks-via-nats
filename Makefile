.PHONY: create-cluster destroy-cluster install-infra create-bucket delete-bucket run run-device-1 run-device-2 run-nats-watcher

create-cluster:
	@echo "Creating cluster..."
	kind create cluster --config ./kind-cluster.yaml

destroy-cluster:
	@echo "Destroying cluster..."
	kind delete cluster --name devicemonitor

install-infra:
	@echo "Adding Helm repos..."
	helm repo add nats https://nats-io.github.io/k8s/helm/charts/ || true
	helm repo update

	@echo "Installing NATS..."
	helm upgrade --install nats nats/nats -n nats --create-namespace -f infra/nats-values.yaml

	@echo "Deploying NATS UI..."
	kubectl apply -f infra/nats-ui.yaml

	@echo "Done."
	@echo "NATS UI: http://localhost:30311"

expose-nats:
	@echo "Exposing NATS..."
	kubectl port-forward svc/nats 4222:4222 -n nats

create-bucket:
	@echo "Creating bucket..."
	nats kv add device-health --ttl 10s --marker-ttl 60s --server nats://localhost:4222

delete-bucket:
	@echo "Deleting bucket..."
	nats kv rm device-health --server nats://localhost:4222

run:
	skaffold dev  --wait-for-deletions=false  --cleanup=false

run-device-1:
	DEVICE_ID=device-1 NATS_URL=nats://localhost:4222 go run cmd/device/main.go

run-device-2:
	DEVICE_ID=device-2 NATS_URL=nats://localhost:4222 go run cmd/device/main.go

run-nats-watcher:
	nats kv watch device-health --server nats://localhost:4222