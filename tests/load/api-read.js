import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    api_reads: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RUNMESH_RATE || 100),
      timeUnit: '1s',
      duration: __ENV.RUNMESH_DURATION || '30s',
      preAllocatedVUs: 40,
      maxVUs: 200,
    },
  },
  // Performance is recorded, while correctness remains blocking.
  thresholds: { checks: ['rate==1'], http_req_failed: ['rate==0'] },
};

const api = __ENV.RUNMESH_API || 'http://localhost:8080';
const token = __ENV.RUNMESH_TOKEN;

export default function () {
  const response = http.get(`${api}/v1/workflows`, { headers: { Authorization: `Bearer ${token}` } });
  check(response, { 'authenticated request succeeded': value => value.status === 200 });
}
