# Demo media

These files are captured from the real local Compose stack, never a mocked dashboard.

To regenerate them from the repository root, start RunMesh and run the pinned
Playwright image. The small TCP forwarder makes the host-published services
available on Docker Desktop while the Docker socket lets the test terminate and
restart only the two local worker containers:

```bash
docker compose up --build -d
docker run --rm --ipc=host \
  -v "$PWD/web:/app" \
  -v "$PWD/docs:/docs" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -w /app mcr.microsoft.com/playwright:v1.56.1-noble \
  sh -lc 'node scripts/forward-host.mjs & proxy_pid=$!; trap "kill $proxy_pid 2>/dev/null || true" EXIT; npm ci >/dev/null && npx playwright test --config=playwright.demo.config.ts'
```

The capture creates a workflow, executes a DAG, terminates both bundled worker
processes during a leased task, restarts the worker containers, waits for lease
recovery, and records the successful second attempt. Review the resulting media
before committing it; never substitute mock data or generated screenshots.
