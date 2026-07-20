import http from 'k6/http';
import { check, sleep } from 'k6';
import { randomUUID } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = { scenarios: { submissions: { executor: 'constant-arrival-rate', rate: 100, timeUnit: '1s', duration: '30s', preAllocatedVUs: 100, maxVUs: 300 } }, thresholds: { http_req_duration: ['p(95)<150'], http_req_failed: ['rate<0.01'] } };
const api = __ENV.RUNMESH_API || 'http://localhost:8080';
const workflow = __ENV.RUNMESH_WORKFLOW_ID;
const token = __ENV.RUNMESH_TOKEN;
export default function () {
  const response = http.post(`${api}/v1/workflows/${workflow}/runs`, JSON.stringify({ input: { value: Math.random() } }), { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'Idempotency-Key': randomUUID() } });
  check(response, { 'created': r => r.status === 201 }); sleep(0.01);
}
