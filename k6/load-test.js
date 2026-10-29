import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
  stages: [
    { duration: '1m', target: 50 }, // Ramp-up to 50 Virtual Users over 1 minutes
    { duration: '2m', target: 50 }, // Sustain 50 Virtual Users for 2 minutes
    { duration: '30s', target: 0 },  // Scale down
  ],
  thresholds: {
    http_req_duration: ['p(95)<200'], // 95% of requests must complete below 200ms
    http_req_failed: ['rate<0.01'],   // Less than 1% failed requests
  },
};

export default function () {
  const BASE_URL = 'http://api-gateway:8000';

  // 1. Pedido GET de consulta de eventos (Catalog)
  const resEvents = http.get(`${BASE_URL}/events`);
  check(resEvents, {
    'GET /events status 200': (r) => r.status === 200,
  });

  sleep(0.5);

  // 2. Pedido POST de criação de encomenda
  const payloadOrder = JSON.stringify({
    customer_id: Math.floor(Math.random() * 1000) + 1,
    event_id: 101,
    total_amount: 85.0,
    quantity: Math.floor(Math.random() * 4) + 1, // entre 1 a 4 bilhetes (limite de 5)
    email: `k6_user_${__VU}@ticketmei.com`,
  });

  const params = {
    headers: { 'Content-Type': 'application/json' },
  };

  const resOrder = http.post(`${BASE_URL}/orders`, payloadOrder, params);
  check(resOrder, {
    'POST /orders status 202': (r) => r.status === 202,
  });

  sleep(1);
}