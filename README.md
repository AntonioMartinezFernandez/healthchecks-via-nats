# Healthchecs via NATS

Install:

- Orbstack
- Kind
- Go
- NATS CLI
- Kubectl
- Helm

Run:

```bash
make create-cluster
make install-infra
make expose-nats
# In another terminal
make create-bucket
make run
# In another terminal
make run-device
```

Connect to NATS via NATS-UI

1. Open `http://localhost:30311`
2. Create a new connection to `nats://nats.nats.svc.cluster.local:4222`
