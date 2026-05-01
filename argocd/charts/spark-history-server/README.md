# Spark History Server Helm Chart

This Helm chart deploys the Spark History Server with S3 support on a Kubernetes cluster.

## Overview

The Spark History Server provides a web UI for viewing metrics and logs for completed and running Spark applications. This chart is configured to read event logs from an S3-compatible object storage (AWS S3, MinIO, etc.).

## Features

- S3/MinIO support for storing Spark event logs
- Configurable log cleaner for automatic cleanup of old logs
- Optional Ingress support for external access
- Optional Horizontal Pod Autoscaling (HPA)
- ServiceMonitor support for Prometheus metrics
- Customizable resource requests and limits
- Health checks (liveness and readiness probes)

## Prerequisites

- Kubernetes 1.19+
- Helm 3.0+
- S3-compatible object storage (AWS S3, MinIO, etc.)
- A bucket created for Spark event logs

## Installing the Chart

### Basic Installation with MinIO

```bash
helm install spark-history-server ./spark-history-server \
  --set s3.enabled=true \
  --set s3.endpoint=http://minio.minio.svc.cluster.local:9000 \
  --set s3.bucket=spark-logs \
  --set s3.accessKeyId=minioadmin \
  --set s3.secretAccessKey=minioadmin \
  --set spark.historyServer.logDirectory=s3a://spark-logs/
```

### Installation with AWS S3

```bash
helm install spark-history-server ./spark-history-server \
  --set s3.enabled=true \
  --set s3.bucket=my-spark-logs \
  --set s3.accessKeyId=AKIAIOSFODNN7EXAMPLE \
  --set s3.secretAccessKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  --set spark.historyServer.logDirectory=s3a://my-spark-logs/
```

### Installation with Existing Secret

```bash
# Create a secret with S3 credentials
kubectl create secret generic spark-s3-creds \
  --from-literal=accesskey=YOUR_ACCESS_KEY \
  --from-literal=secretkey=YOUR_SECRET_KEY

# Install the chart
helm install spark-history-server ./spark-history-server \
  --set s3.enabled=true \
  --set s3.endpoint=http://minio.minio.svc.cluster.local:9000 \
  --set s3.bucket=spark-logs \
  --set s3.existingSecret=spark-s3-creds \
  --set spark.historyServer.logDirectory=s3a://spark-logs/
```

## Configuration

The following table lists the configurable parameters of the Spark History Server chart and their default values.

### General Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | Image repository | `apache/spark` |
| `image.tag` | Image tag | `3.5.1` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `nameOverride` | Override chart name | `""` |
| `fullnameOverride` | Override full chart name | `""` |

### Service Account

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serviceAccount.create` | Create service account | `true` |
| `serviceAccount.annotations` | Service account annotations | `{}` |
| `serviceAccount.name` | Service account name | `""` |

### Service

| Parameter | Description | Default |
|-----------|-------------|---------|
| `service.type` | Service type | `ClusterIP` |
| `service.port` | Service port | `18080` |
| `service.targetPort` | Container port | `18080` |
| `service.annotations` | Service annotations | `{}` |

### Ingress

| Parameter | Description | Default |
|-----------|-------------|---------|
| `ingress.enabled` | Enable ingress | `false` |
| `ingress.className` | Ingress class name | `""` |
| `ingress.annotations` | Ingress annotations | `{}` |
| `ingress.hosts` | Ingress hosts | See `values.yaml` |
| `ingress.tls` | Ingress TLS configuration | `[]` |

### Resources

| Parameter | Description | Default |
|-----------|-------------|---------|
| `resources.limits.cpu` | CPU limit | `1000m` |
| `resources.limits.memory` | Memory limit | `2Gi` |
| `resources.requests.cpu` | CPU request | `500m` |
| `resources.requests.memory` | Memory request | `1Gi` |

### Spark History Server Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `spark.historyServer.logDirectory` | S3 path for event logs | `s3a://spark-logs/` |
| `spark.historyServer.ui.port` | UI port | `18080` |
| `spark.historyServer.cleaner.enabled` | Enable log cleaner | `true` |
| `spark.historyServer.cleaner.interval` | Cleaner interval | `1d` |
| `spark.historyServer.cleaner.maxAge` | Maximum age of logs | `7d` |
| `spark.historyServer.extraConf` | Additional Spark configuration | `{}` |

### S3 Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `s3.enabled` | Enable S3 support | `true` |
| `s3.endpoint` | S3 endpoint (empty for AWS S3) | `""` |
| `s3.bucket` | S3 bucket name | `spark-logs` |
| `s3.accessKeyId` | S3 access key ID | `""` |
| `s3.secretAccessKey` | S3 secret access key | `""` |
| `s3.existingSecret` | Use existing secret for credentials | `""` |
| `s3.pathStyleAccess` | Use path-style access | `true` |
| `s3.ssl.enabled` | Enable SSL for S3 | `false` |
| `s3.extraConf` | Additional S3 configuration | `{}` |

### Autoscaling

| Parameter | Description | Default |
|-----------|-------------|---------|
| `autoscaling.enabled` | Enable HPA | `false` |
| `autoscaling.minReplicas` | Minimum replicas | `1` |
| `autoscaling.maxReplicas` | Maximum replicas | `3` |
| `autoscaling.targetCPUUtilizationPercentage` | Target CPU utilization | `80` |
| `autoscaling.targetMemoryUtilizationPercentage` | Target memory utilization | `80` |

### ServiceMonitor

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serviceMonitor.enabled` | Enable ServiceMonitor | `false` |
| `serviceMonitor.interval` | Scrape interval | `30s` |
| `serviceMonitor.scrapeTimeout` | Scrape timeout | `10s` |

## Configuring Spark Applications

To enable event logging in your Spark applications, configure them with the following settings:

```bash
spark-submit \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=s3a://spark-logs/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio.minio.svc.cluster.local:9000 \
  --conf spark.hadoop.fs.s3a.access.key=YOUR_ACCESS_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=YOUR_SECRET_KEY \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  your-application.py
```

Or in your Spark application code:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "s3a://spark-logs/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.minio.svc.cluster.local:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
```

## Accessing the History Server

### Using Port Forward

```bash
kubectl port-forward svc/spark-history-server 18080:18080
```

Then navigate to http://localhost:18080

### Using Ingress

Enable ingress in your values:

```yaml
ingress:
  enabled: true
  className: nginx
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
  hosts:
    - host: spark-history.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: spark-history-tls
      hosts:
        - spark-history.example.com
```

## Upgrading

```bash
helm upgrade spark-history-server ./spark-history-server \
  --values my-values.yaml
```

## Uninstalling

```bash
helm uninstall spark-history-server
```

## Troubleshooting

### Check if the History Server is running

```bash
kubectl get pods -l app.kubernetes.io/name=spark-history-server
```

### View logs

```bash
kubectl logs -l app.kubernetes.io/name=spark-history-server
```

### Verify S3 connectivity

```bash
kubectl exec -it <pod-name> -- bash
# Inside the pod
aws s3 ls s3://spark-logs/ --endpoint-url=http://minio.minio.svc.cluster.local:9000
```

### Common Issues

1. **No event logs showing**: Ensure your Spark applications are configured to write event logs to the same S3 location
2. **S3 access denied**: Verify your S3 credentials and bucket permissions
3. **Pod not starting**: Check resource limits and ensure the S3 endpoint is accessible from the cluster

## License

Apache License 2.0
