import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '5s', target: 20 },  // Ramp-up to 20 users
    { duration: '10s', target: 50 }, // Sustained load 50 users
    { duration: '5s', target: 0 },   // Ramp-down
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // 95% of requests must finish within 500ms
    http_req_failed: ['rate<0.01'],   // Error rate must be under 1%
  },
};

export default function () {
  const url = 'http://localhost:8000/api/v1/events';
  const payload = JSON.stringify({
    event_type: 'payment.succeeded',
    payload: {
      amount: 4999,
      currency: 'USD',
      customer: `cust_${__VU}_${__ITER}`
    },
    idempotency_key: `k6-idem-${__VU}-${__ITER}`
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-Idempotency-Key': `k6-idem-${__VU}-${__ITER}`
    },
  };

  const res = http.post(url, payload, params);

  check(res, {
    'status is 202': (r) => r.status === 202,
    'event_id present': (r) => r.json().event_id !== undefined,
  });

  sleep(0.1);
}
