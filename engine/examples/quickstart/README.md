# Flapjack Quickstart (Docker Compose)

Single-node local setup that builds Flapjack from this repo (`build:` source in compose).
This example disables API auth and publishes port `7700`, so use it only on a trusted local machine. Do not expose it to shared networks, port-forward it, or treat it as a production deployment.

```bash
docker compose up -d --build
curl -sf http://localhost:7700/health
```

```bash
docker compose down -v
```

For production-style multi-node topology, see [`../ha-cluster/`](../ha-cluster/).
