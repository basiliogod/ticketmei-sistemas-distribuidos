import http from "k6/http";
import { check } from "k6";
import { Counter, Trend } from "k6/metrics";

// Métricas personalizadas para contagem e comparação de latência
const sagaSuccessCount = new Counter("saga_success_201_total");
const sagaCompensatedCount = new Counter("saga_compensated_or_aborted_total");
const sagaUnexpectedCount = new Counter("saga_unexpected_errors_total");

const sagaSuccessLatency = new Trend("saga_success_latency_ms");
const sagaCompensatedLatency = new Trend("saga_compensated_latency_ms");

const BASE_URL = __ENV.BASE_URL || "http://host.docker.internal:8000";
const INVENTORY_URL = __ENV.INVENTORY_URL || "http://host.docker.internal:8005";
const ORDER_URL = __ENV.ORDER_URL || "http://host.docker.internal:8002";
const PAYMENT_URL = __ENV.PAYMENT_URL || "http://host.docker.internal:8004";

const TEST_EVENT_ID = 999;
const INITIAL_TICKETS = 20;

export const options = {
  scenarios: {
    // 1.000 VUs simultâneos a tentar comprar 1 bilhete no mesmo segundo
    concurrent_checkout_spike: {
      executor: "per-vu-iterations",
      vus: 1000,
      iterations: 1,
      maxDuration: "1m",
    },
  },
  thresholds: {
    // Valida que exatamente 20 encomendas tiveram sucesso e 980 foram abortadas/compensadas
    saga_success_201_total: ["count == 20"],
    saga_compensated_or_aborted_total: ["count == 980"],
    saga_unexpected_errors_total: ["count == 0"],
  },
};

// 1. Configurar o evento com apenas 20 bilhetes disponíveis
export function setup() {
  const headers = { "Content-Type": "application/json" };

  // Garantir que o Payment está saudável (0% falha, 0ms atraso) para testar puramente a concorrência de stock
  http.post(
    `${PAYMENT_URL}/payments/configure-faults`,
    JSON.stringify({ error_rate: 0, delay_ms: 0 }),
    { headers },
  );

  // Configurar stock inicial = 20 no Inventory
  const resStock = http.post(
    `${INVENTORY_URL}/inventory/stock`,
    JSON.stringify({
      event_id: TEST_EVENT_ID,
      total_tickets: INITIAL_TICKETS,
      available_tickets: INITIAL_TICKETS,
    }),
    { headers },
  );

  const stockConfigured = check(resStock, {
    "Setup: Stock inicial (20 bilhetes) configurado com sucesso": (r) =>
      r.status === 200 || r.status === 201,
  });

  if (!stockConfigured) {
    throw new Error(
      `Falha ao configurar stock no Inventory: HTTP ${resStock.status} - ${resStock.body}`,
    );
  }

  console.log(
    `Evento ${TEST_EVENT_ID} configurado com ${INITIAL_TICKETS} bilhetes disponíveis.`,
  );
  return { eventId: TEST_EVENT_ID };
}

// 2. EXECUÇÃO CONCORRENTE (1.000 VUs no mesmo instante)
export default function (data) {
  const payload = JSON.stringify({
    customer_id: __VU,
    event_id: data.eventId,
    total_amount: 50.0,
    quantity: 1,
    email: `saga_vu_${__VU}@ticketmei.com`,
  });

  const params = {
    headers: { "Content-Type": "application/json" },
    timeout: "15s",
  };

  const res = http.post(`${BASE_URL}/orders`, payload, params);

  if (res.status === 201) {
    // Saga concluída com sucesso (Reserva -> Pagamento -> Confirmação)
    sagaSuccessCount.add(1);
    sagaSuccessLatency.add(res.timings.duration);
  } else if (
    res.status === 409 ||
    res.status === 400 ||
    res.status === 502 ||
    res.status === 503
  ) {
    // Saga abortada por falta de stock ou compensada
    sagaCompensatedCount.add(1);
    sagaCompensatedLatency.add(res.timings.duration);
  } else {
    sagaUnexpectedCount.add(1);
    console.error(
      `[VU ${__VU}] Erro inesperado: HTTP ${res.status} - ${res.body}`,
    );
  }

  check(res, {
    "Saga Concluída (201)": (r) => r.status === 201,
    "Saga Abortada/Compensada (409/400/502/503)": (r) =>
      [409, 400, 502, 503].includes(r.status),
  });
}

// 3. Validar consistência final entre Inventory, Order e Payment
export function teardown(data) {
  console.log("\nA auditar consistência entre os microsserviços...");

  // Consultar estado final no Inventory
  const resInv = http.get(`${INVENTORY_URL}/inventory/${data.eventId}`);
  if (resInv.status === 200) {
    const invData = JSON.parse(resInv.body);
    console.log(
      `Evento ${data.eventId}: Total=${invData.total_tickets} | Disponíveis=${invData.available_tickets}`,
    );

    check(invData, {
      "Consistência Inventory: Stock final é exatamente 0 ": (
        d,
      ) => d.available_tickets === 0,
    });
  } else {
    console.warn(
      `Não foi possível ler stock final: HTTP ${resInv.status}`,
    );
  }

  // Consultar métricas Prometheus dos serviços para cruzar os totais exatos
  const orderMetrics = http.get(`${ORDER_URL}/metrics`);
  if (orderMetrics.status === 200) {
    const initiatedMatch = orderMetrics.body.match(
      /saga_transactions_initiated_total(?:\{[^}]*\})?\s+(\d+)/,
    );
    const successMatch = orderMetrics.body.match(
      /saga_transactions_success_total(?:\{[^}]*\})?\s+(\d+)/,
    );
    const abortedMatch = orderMetrics.body.match(
      /saga_transactions_aborted_total(?:\{[^}]*\})?\s+(\d+)/,
    );
    console.log(
      `Iniciadas=${initiatedMatch ? initiatedMatch[1] : "?"} | Sucesso=${
        successMatch ? successMatch[1] : "?"
      } | Abortadas=${abortedMatch ? abortedMatch[1] : "?"}`,
    );
  }
}
