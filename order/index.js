const express = require('express');
const amqp = require('amqplib');
const client = require('prom-client');

client.collectDefaultMetrics();

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 8002;
const PAYMENT_URL = process.env.PAYMENT_URL || 'http://payment:8004';
const RABBITMQ_URL = process.env.RABBITMQ_HOST
  ? `amqp://${encodeURIComponent(process.env.RABBITMQ_USER)}:${encodeURIComponent(process.env.RABBITMQ_PASSWORD)}@${process.env.RABBITMQ_HOST}:5672${encodeURIComponent(process.env.RABBITMQ_VHOST)}`
  : (process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672');

const EXCHANGE_NAME = 'orders.exchange';
const QUEUE_NAME = 'order_notifications';

const ordersInitiatedTotal = new client.Counter({
  name: 'orders_initiated_total',
  help: 'Total de encomendas iniciadas no servico Order',
  labelNames: ['status'],
});

const circuitBreakerStateGauge = new client.Gauge({
  name: 'circuit_breaker_state',
  help: 'Estado do Circuit Breaker: 0=Closed, 1=Open, 2=Half-Open',
});

const circuitBreakerFallbacksTotal = new client.Counter({
  name: 'circuit_breaker_fallbacks_total',
  help: 'Total de vezes que o fallback do Circuit Breaker foi acionado',
});

const paymentClientRequestsTotal = new client.Counter({
  name: 'payment_client_requests_total',
  help: 'Total de chamadas HTTP feitas ao servico Payment',
  labelNames: ['status'],
});

// ============================================================================
// CONFIGURAÇÃO DO CIRCUIT BREAKER & RETRY COM JITTER
// ============================================================================
const CB_STATE = {
  CLOSED: 0,
  OPEN: 1,
  HALF_OPEN: 2,
};

const cbConfig = {
  failureThreshold: parseInt(process.env.CB_FAILURE_THRESHOLD || '5', 10), // Falhas para abrir
  resetTimeoutMs: parseInt(process.env.CB_RESET_TIMEOUT_MS || '10000', 10), // Tempo em OPEN antes de testar HALF-OPEN (10s)
  requestTimeoutMs: parseInt(process.env.PAYMENT_TIMEOUT_MS || '2000', 10), // Timeout por tentativa (2s)
  maxRetries: parseInt(process.env.PAYMENT_MAX_RETRIES || '2', 10), // Retries antes de contabilizar falha no CB
  baseDelayMs: parseInt(process.env.PAYMENT_RETRY_BASE_MS || '100', 10), // Base para backoff + jitter
};

let cbState = CB_STATE.CLOSED;
let consecutiveFailures = 0;
let nextAttemptTime = 0;
let halfOpenProbeInFlight = false;

circuitBreakerStateGauge.set(cbState);

function setCircuitState(newState) {
  if (cbState !== newState) {
    console.log(`[CircuitBreaker] Transição de estado: ${cbState} -> ${newState}`);
    cbState = newState;
    circuitBreakerStateGauge.set(cbState);
  }
}

function createCircuitOpenError() {
  circuitBreakerFallbacksTotal.inc();
  const err = new Error('CIRCUIT_OPEN');
  err.isCircuitOpen = true;
  return err;
}

// Função auxiliar para aguardar com Exponential Backoff + Full Jitter
function sleepWithJitter(attempt) {
  const expBackoff = cbConfig.baseDelayMs * Math.pow(2, attempt);
  const jitter = Math.floor(Math.random() * expBackoff);
  return new Promise((resolve) => setTimeout(resolve, jitter));
}

// Executa uma chamada HTTP individual ao Payment com Timeout
async function singlePaymentRequest(payload) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), cbConfig.requestTimeoutMs);

  try {
    const response = await fetch(`${PAYMENT_URL}/payments`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    clearTimeout(timeoutId);

    if (response.ok) {
      paymentClientRequestsTotal.inc({ status: '2xx' });
      return await response.json();
    } else {
      paymentClientRequestsTotal.inc({ status: String(response.status) });
      throw new Error(`Payment HTTP ${response.status}`);
    }
  } catch (err) {
    clearTimeout(timeoutId);
    if (err.name === 'AbortError') {
      paymentClientRequestsTotal.inc({ status: 'timeout' });
      throw new Error('Payment timeout');
    }
    if (!err.message.startsWith('Payment HTTP')) {
      paymentClientRequestsTotal.inc({ status: '500' });
    }
    throw err;
  }
}

// Executa a chamada protegida por Circuit Breaker + Retry com Jitter
async function processPaymentWithResilience(payload) {
  // 1. Verificar se o circuito está OPEN
  if (cbState === CB_STATE.OPEN) {
    if (Date.now() >= nextAttemptTime) {
      setCircuitState(CB_STATE.HALF_OPEN);
      halfOpenProbeInFlight = false;
    } else {
      // Fail-fast: não faz pedidos ao Payment nem retries
      throw createCircuitOpenError();
    }
  }

  let isProbeRequest = false;
  if (cbState === CB_STATE.HALF_OPEN) {
    if (halfOpenProbeInFlight) {
      throw createCircuitOpenError();
    }
    halfOpenProbeInFlight = true;
    isProbeRequest = true;
  }

  const attemptsAllowed = isProbeRequest ? 1 : cbConfig.maxRetries + 1;
  let lastError;

  for (let attempt = 0; attempt < attemptsAllowed; attempt++) {
    if (cbState === CB_STATE.OPEN) {
      if (isProbeRequest) halfOpenProbeInFlight = false;
      throw createCircuitOpenError();
    }

    try {
      const result = await singlePaymentRequest(payload);
      consecutiveFailures = 0;
      if (isProbeRequest || cbState === CB_STATE.HALF_OPEN) {
        halfOpenProbeInFlight = false;
        setCircuitState(CB_STATE.CLOSED);
      }
      return result;
    } catch (err) {
      lastError = err;
      if (attempt < attemptsAllowed - 1 && cbState === CB_STATE.CLOSED) {
        await sleepWithJitter(attempt);
      }
    }
  }

  if (isProbeRequest) {
    halfOpenProbeInFlight = false;
  }
  if (cbState === CB_STATE.OPEN) {
    throw createCircuitOpenError();
  }

  consecutiveFailures++;

  if (cbState === CB_STATE.HALF_OPEN || consecutiveFailures >= cbConfig.failureThreshold) {
    setCircuitState(CB_STATE.OPEN);
    nextAttemptTime = Date.now() + cbConfig.resetTimeoutMs;
    throw createCircuitOpenError();
  }

  throw lastError;
}

// ============================================================================
// BASE DE DADOS EM MEMÓRIA E RABBITMQ
// ============================================================================
const ordersDb = new Map();
let currentOrderId = 1;
let rabbitChannel = null;

async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    rabbitChannel = await connection.createChannel();
    await rabbitChannel.assertExchange(EXCHANGE_NAME, 'fanout', { durable: true });
    await rabbitChannel.assertQueue(QUEUE_NAME, { durable: true });
    await rabbitChannel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, '');
    console.log('Order: Conectado ao RabbitMQ (orders.exchange)');
  } catch (err) {
    console.error('Order: Erro a conectar ao RabbitMQ, tentando em 3s...', err.message);
    setTimeout(connectRabbitMQ, 3000);
  }
}
connectRabbitMQ();

// ============================================================================
// ENDPOINTS HTTP
// ============================================================================
app.post('/orders', async (req, res) => {
  const { customer_id, event_id, total_amount, quantity, email } = req.body;

  // Validação do limite de bilhetes por compra (RF2: max 4)
  if (quantity && quantity > 4) {
    return res.status(400).json({ error: 'Limite maximo de 4 bilhetes por compra excedido.' });
  }

  const orderId = currentOrderId++;
  const order = {
    id: orderId,
    customer_id: customer_id || 1,
    event_id: event_id || 101,
    created_at: new Date().toISOString(),
    total_amount: total_amount || 0.0,
    quantity: quantity || 1,
    status: 'PENDING',
  };

  ordersDb.set(orderId, order);

  try {
    await processPaymentWithResilience({
      order_id: order.id,
      amount: order.total_amount,
    });

    order.status = 'CONFIRMED';
    ordersDb.set(orderId, order);
    ordersInitiatedTotal.inc({ status: 'CONFIRMED' });

    const messagePayload = {
      event: 'OrderCreated',
      order_id: order.id,
      customer_id: order.customer_id,
      email: email || `customer_${order.customer_id}@ticketmei.com`,
      total_amount: order.total_amount,
    };

    if (rabbitChannel) {
      rabbitChannel.publish(
        EXCHANGE_NAME,
        '',
        Buffer.from(JSON.stringify(messagePayload)),
        { persistent: true }
      );
    } else {
      console.warn('Order: Canal RabbitMQ indisponivel no momento.');
    }

    return res.status(201).json({
      message: 'Encomenda confirmada com sucesso.',
      order,
    });
  } catch (error) {
    order.status = 'FAILED';
    ordersDb.set(orderId, order);
    ordersInitiatedTotal.inc({ status: 'FAILED' });

    if (error.isCircuitOpen) {
      return res.status(503).json({
        error: 'Payment system unavailable, try again later',
      });
    }

    return res.status(502).json({
      error: 'Falha ao processar pagamento.',
      order,
    });
  }
});

app.get('/orders/:id', (req, res) => {
  const id = parseInt(req.params.id, 10);
  const order = ordersDb.get(id);

  if (!order) {
    return res.status(404).json({ error: 'Order nao encontrada.' });
  }

  return res.json(order);
});

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', client.register.contentType);
  res.end(await client.register.metrics());
});

app.listen(PORT, () => {
  console.log(`Order Service a correr na porta ${PORT}`);
});