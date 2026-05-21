# Kubernetes Local POC

This folder contains a local Kubernetes proof of concept for the Peakload API service. It shows that the API can run as a Deployment, be exposed through a ClusterIP Service, and be targeted by a Horizontal Pod Autoscaler.

## Scope and Limits

- This is not a production deployment.
- This is not an AWS, EKS, or cloud deployment.
- Docker Compose remains the main implementation for the capstone.
- PostgreSQL Patroni, Redis Sentinel, RabbitMQ, Prometheus, and Grafana stay in Docker Compose.
- The Kubernetes manifests only run the API container and connect it to dependencies that are already running outside the cluster.

## Build the Local Image

```sh
docker build -t peakload-capstone:local .
```

For clusters that do not share the host Docker image store, load this image into the local cluster first, for example with `kind load docker-image peakload-capstone:local`.

## Configure Secrets

Copy `secret.example.yaml` to a local secret manifest and replace every `CHANGEME` value with your local Docker Compose connection values. Keep real secret files out of git.

```sh
cp k8s/secret.example.yaml k8s/secret.local.yaml
```

If you use a separate local secret file, apply it before or together with the other manifests and keep `secret.example.yaml` as documentation.

## Connecting Kubernetes POC to Docker Compose Dependencies

Docker Compose remains the main full-stack implementation. This Kubernetes POC only runs the API pod through the local Deployment, Service, and HPA manifests.

The API pod reaches PostgreSQL, Redis, and RabbitMQ dependencies that are still running in Docker Compose through `host.docker.internal`. For that to work, Docker Compose must expose the required dependency ports on the host:

- PostgreSQL shard HAProxy ports: `5000`, `5001`
- Redis master port: `6379`
- Redis replica read port: `6380`
- Redis Sentinel ports: `26379`, `26380`, `26381`
- RabbitMQ AMQP port: `5672`

This setup is only for a local demo and is not a production Kubernetes configuration. Do not commit real passwords; copy `secret.example.yaml` to a local secret manifest and keep password values as local-only secrets.

If `REDIS_READ_URL` is not used by the API or the Redis replica is not available in your local run, it is acceptable for this POC to point `REDIS_READ_URL` at the Redis master on `host.docker.internal:6379`.

## Apply

```sh
kubectl apply -k k8s/
```

## Check

```sh
kubectl get all -n peakload
kubectl get hpa -n peakload
```

## Manual Scale Test

```sh
kubectl scale deployment peakload-api -n peakload --replicas=3
```

## Port Forward

```sh
kubectl port-forward -n peakload svc/peakload-api 8088:80
```

Then open `http://localhost:8088/health`.
