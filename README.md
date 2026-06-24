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
