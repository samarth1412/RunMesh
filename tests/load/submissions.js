import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    submissions: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RUNMESH_RATE || 50),
      timeUnit: '1s',
      duration: __ENV.RUNMESH_DURATION || '30s',
      preAllocatedVUs: 100,
      maxVUs: 300,
    },
  },
  // Latency is evidence, not a merge gate. Correctness remains blocking.
  thresholds: { checks: ['rate==1'], http_req_failed: ['rate==0'] },
};
const api = __ENV.RUNMESH_API || 'http://localhost:8080';
const workflow = __ENV.RUNMESH_WORKFLOW_ID;
const token = __ENV.RUNMESH_TOKEN;
export default function () {
  const key = `benchmark-${__VU}-${__ITER}-${Date.now()}`;
  const response = http.post(`${api}/v1/workflows/${workflow}/runs`, JSON.stringify({ input: { value: Math.random() } }), { headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'Idempotency-Key': key } });
  check(response, { 'created': r => r.status === 201 });
}
