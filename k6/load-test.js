import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter } from 'k6/metrics';

// Contadores personalizados para distinguir o resultado na consola
const successfulRequests = new Counter('successful_requests');
const rateLimitedRequests = new Counter('rate_limited_requests_429');

export const options = {
  stages: [
    { duration: '30s', target: 20 },
    { duration: '30s', target: 80 },
    { duration: '30s', target: 0 },
  ],
  thresholds: {
    http_req_duration: ['p(95)<200'],
  },
};

export default function () {
  const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

  // 1. Pedido GET de consulta de eventos (Catalog)
  const resEvents = http.get(`${BASE_URL}/events`);
  
  if (resEvents.status === 200) {
    successfulRequests.add(1);
  } else if (resEvents.status === 429) {
    rateLimitedRequests.add(1);
  }

  check(resEvents, {
    'GET /events status 200 (Success)': (r) => r.status === 200,
    'GET /events status 429 (Rate Limited)': (r) => r.status === 429,
  });

  sleep(0.5);

  // 2. Pedido POST de criação de encomenda
  const payloadOrder = JSON.stringify({
    customer_id: Math.floor(Math.random() * 1000) + 1,
    event_id: 101,
    total_amount: 85.0,
    quantity: Math.floor(Math.random() * 4) + 1,
    email: `k6_user_${__VU}@ticketmei.com`,
  });

  const params = {
    headers: { 'Content-Type': 'application/json' },
  };

  const resOrder = http.post(`${BASE_URL}/orders`, payloadOrder, params);

  if (resOrder.status === 202) {
    successfulRequests.add(1);
  } else if (resOrder.status === 429) {
    rateLimitedRequests.add(1);
  }

  check(resOrder, {
    'POST /orders status 202 (Accepted)': (r) => r.status === 202,
    'POST /orders status 429 (Rate Limited)': (r) => r.status === 429,
  });

  sleep(0.5);
}