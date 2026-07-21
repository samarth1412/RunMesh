# Production deployment and releases

RunMesh ships a Helm chart for the control plane, scheduler, dashboard, and any number of Go or Python worker pools. PostgreSQL, Redis, object storage, OIDC, and Kafka are external dependencies. Kafka is intentionally not provisioned by the Terraform module.

## Helm

Create a Kubernetes Secret containing the runtime credentials:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: runmesh-production
  namespace: runmesh
type: Opaque
stringData:
  RUNMESH_DATABASE_URL: postgres://...
  RUNMESH_REDIS_URL: rediss://...
  RUNMESH_INTERNAL_TOKEN: ...
  RUNMESH_API_KEY_PEPPER: ...
  RUNMESH_ARTIFACT_ACCESS_KEY: ""
  RUNMESH_ARTIFACT_SECRET_KEY: ""
```

On EKS, prefer IRSA for artifact access and leave the two artifact credential values empty. Install with explicit image versions and production endpoints:

```bash
helm upgrade --install runmesh deploy/helm/runmesh \
  --namespace runmesh --create-namespace \
  --set secret.existingSecret=runmesh-production \
  --set config.kafkaBrokers=kafka.example.internal:9092 \
  --set config.oidcIssuer=https://identity.example.com/realms/runmesh \
  --set config.oidcJwksURL=https://identity.example.com/realms/runmesh/protocol/openid-connect/certs \
  --set config.corsAllowlist=https://runmesh.example.com \
  --set config.artifactRegion=us-east-1 \
  --set config.artifactBucket=runmesh-production-artifacts \
  --set images.controlPlane.tag=0.1.0 \
  --set images.scheduler.tag=0.1.0 \
  --set images.web.tag=0.1.0 \
  --set images.workerGo.tag=0.1.0
```

The dashboard reads OIDC and API configuration from a ConfigMap-mounted `config.js`, so the same immutable web image can be promoted between environments. Credentials are rendered only into a Secret, never into the ConfigMap. Probes, resources, HPAs, disruption budgets, restricted security contexts, rolling strategies, and default-deny NetworkPolicies are enabled by default.

Worker credentials should normally use a separate Secret per pool with an API key carrying only `workers:execute`. Set `workerPools[].credentialSecretName` to that Secret and `credentialSecretKey` to its key. Add pools by appending entries with `type: go` or `type: python`.

Database migrations are an explicit pre-deployment step. Run the versioned files in `migrations/` before upgrading application pods.

## AWS validation

The Terraform module creates EKS, RDS PostgreSQL, encrypted ElastiCache Redis, an encrypted/versioned S3 artifact bucket, five ECR repositories with lifecycle rules, CloudWatch integration, and IRSA roles for RunMesh artifact access and the AWS Load Balancer Controller. Route 53 and ACM are optional. Supply `kafka_brokers`; the module does not create MSK.

```bash
terraform -chdir=infra/terraform init
terraform -chdir=infra/terraform plan -var='kafka_brokers=kafka.example.internal:9092'
```

Apply is intentionally manual. No AWS resources are created by CI. The mock-provider test validates the production graph without credentials:

```bash
terraform -chdir=infra/terraform test
```

Annotate the chart service account with the `artifact_role_arn` output. Install the AWS Load Balancer Controller separately using `load_balancer_controller_role_arn`, then enable the chart ingress with the appropriate controller annotations. If DNS is managed elsewhere, leave the Route 53 variables empty.

## Release and upgrade validation

Tags matching `v*` are configured to publish control-plane, scheduler, Go worker, Python worker, and web images to GHCR. Every image receives semantic-version and commit-SHA tags, a multi-architecture manifest, SBOM, provenance attestation, and a Trivy scan. No `v0.1.0` tag or image is published as part of repository validation; a maintainer must explicitly create the tag after reviewing the release checklist.

CI lints every workflow with actionlint, validates Terraform and its mock plan, lints/templates the Helm chart, checks manifests with kubeconform, and runs a kind rolling-upgrade scenario while a workflow is active. Run that scenario locally with:

```bash
tests/deployment/kind-rolling-upgrade.sh
```

The test creates and removes a dedicated `runmesh-upgrade` kind cluster. It never contacts AWS.
