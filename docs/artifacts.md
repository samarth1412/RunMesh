# Artifacts

RunMesh stores inputs, outputs, and task logs in an S3-compatible bucket. Metadata and ownership remain in PostgreSQL; API callers never submit arbitrary object URIs.

Create an upload with `POST /v1/artifacts/uploads`, upload exactly the declared bytes to the returned 15-minute presigned URL, then call `POST /v1/artifacts/{id}/complete`. Completion verifies object size and the optional SHA-256 checksum. Ready input artifacts can be passed to `POST /v1/workflows/{id}/runs` as `input_artifact_id`. Run details contain short-lived, tenant-authorized download links.

`RUNMESH_ARTIFACT_ENDPOINT` is the private S3 endpoint used by the control plane. When clients cannot resolve that hostname, set `RUNMESH_ARTIFACT_PUBLIC_ENDPOINT` to the externally reachable endpoint used only for presigned transfer URLs. Compose uses `minio:9000` privately and `localhost:9000` publicly.

Input and output artifacts are limited to 100 MiB, logs to 10 MiB. The Go and Python workers download artifact-backed inputs, expose an upload helper, upload structured attempt logs, and offload JSON outputs larger than 256 KiB automatically. Upload timeouts are retryable worker failures and leave the attempt and pending artifact metadata intact.

Local Compose uses path-style MinIO. Configure production S3 with `RUNMESH_ARTIFACT_REGION`, `RUNMESH_ARTIFACT_BUCKET`, and workload credentials. `RUNMESH_ARTIFACT_ENDPOINT` and `RUNMESH_ARTIFACT_PATH_STYLE=true` are intended for MinIO and compatible development services.
