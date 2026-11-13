import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';

// Métricas para distinguir respostas normais (201) vs Circuit Breaker aberto (503)
const ordersConfirmed201 = new Counter('orders_confirmed_201');
const circuitOpen503 = new Counter('circuit_breaker_open_503');
const otherErrors = new Counter('unexpected_errors');
const orderLatency = new Trend('order_request_latency_ms');

export const options = {
  scenarios: {
    // 1. Carga constante de 50 pedidos/s no POST /orders durante 3m30s (cenário 1)
    constant_order_load: {
      executor: 'constant-arrival-rate',
      rate: 50,
      timeUnit: '1s',
      duration: '3m30s',
      preAllocatedVUs: 50,
      maxVUs: 250,
      exec: 'sendOrder',
    },
    // 2. Orquestrador de injeção de falhas no Payment (cenário 2)
    fault_injector: {
      executor: 'per-vu-iterations',
      vus: 1,
      iterations: 1,
      maxDuration: '3m30s',
      exec: 'injectFaults',
    },
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://host.docker.internal:8000';
const PAYMENT_URL = __ENV.PAYMENT_URL || 'http://host.docker.internal:8004';

// Função executada a 50 req/s
export function sendOrder() {
  const payload = JSON.stringify({
    customer_id: Math.floor(Math.random() * 1000) + 1,
    event_id: 101,
    total_amount: 85.0,
    quantity: Math.floor(Math.random() * 4) + 1,
    email: `cb_test_${__VU}_${__ITER}@ticketmei.com`,
  });

  const params = {
    headers: { 'Content-Type': 'application/json' },
    timeout: '12s',
  };

  const res = http.post(`${BASE_URL}/orders`, payload, params);
  orderLatency.add(res.timings.duration);

  if (res.status === 201) {
    ordersConfirmed201.add(1);
  } else if (res.status === 503) {
    circuitOpen503.add(1);
  } else {
    otherErrors.add(1);
  }

  check(res, {
    'Order Confirmed (201)': (r) => r.status === 201,
    'Circuit Breaker Open / Fail-Fast (503)': (r) => r.status === 503,
  });
}

// Função executada 1 vez para controlar a saúde do Payment
export function injectFaults() {
  const headers = { 'Content-Type': 'application/json' };

  // Garantir estado inicial limpo
  http.post(`${PAYMENT_URL}/payments/configure-faults`, JSON.stringify({
    error_rate: 0,
    delay_ms: 0,
  }), { headers });

  console.log('[00:00] Sistema saudável (Circuit CLOSED). A aguardar 30s ...');
  sleep(30);

  // Injetar degradação a meio do teste durante 2 minutos
  console.log('[00:30] A injetar falhas no Payment (80% erro 500 + 10s atraso) durante 2 minutos... (CLOSED -> OPEN)');
  const resDegrade = http.post(`${PAYMENT_URL}/payments/configure-faults`, JSON.stringify({
    error_rate: 0.8,
    delay_ms: 10000,
  }), { headers });
  check(resDegrade, { 'Faults activated (200)': (r) => r.status === 200 });

  sleep(120);

  // Restaurar o serviço Payment
  console.log('[02:30] A restaurar Payment (0% erro, 0ms atraso). A observar recuperação (SEMI-ABERTO -> CLOSED)...');
  const resRecover = http.post(`${PAYMENT_URL}/payments/configure-faults`, JSON.stringify({
    error_rate: 0,
    delay_ms: 0,
  }), { headers });
  check(resRecover, { 'Payment recovered (200)': (r) => r.status === 200 });

  sleep(60);
  console.log('[03:30] Teste concluído.');
}