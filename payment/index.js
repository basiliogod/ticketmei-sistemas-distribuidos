const express = require('express');
const client = require('prom-client');

client.collectDefaultMetrics();

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 8004;

// ============================================================================
// MÉTRICAS PROMETHEUS
// ============================================================================
const paymentRequestsTotal = new client.Counter({
  name: 'payment_requests_total',
  help: 'Total de pedidos de pagamento recebidos pelo Payment',
  labelNames: ['http_status'],
});

const paymentProcessingDurationSeconds = new client.Histogram({
  name: 'payment_processing_duration_seconds',
  help: 'Duracao do processamento de pagamentos em segundos',
  buckets: [0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15],
});

// Middleware de métricas: conta exclusivamente os pedidos para POST /payments
app.use('/payments', (req, res, next) => {
  if (req.method !== 'POST' || req.path !== '/') return next();
  const endTimer = paymentProcessingDurationSeconds.startTimer();
  res.on('finish', () => {
    endTimer();
    paymentRequestsTotal.inc({ http_status: res.statusCode.toString() });
  });
  next();
});

// ============================================================================
// INJEÇÃO DE FALHAS (POST /payments/configure-faults)
// ============================================================================
const faultConfig = {
  error_rate: parseFloat(process.env.FAULT_ERROR_RATE || '0'), // Probabilidade de erro 500 (0..1)
  delay_ms: parseInt(process.env.FAULT_DELAY_MS || '0', 10),  // Latência extra em ms
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ============================================================================
// BASE DE DADOS EM MEMÓRIA (payment_db)
// Modelo: Id (PK integer), Order_id (integer), amount (money), Status (SUCCESS, FAILED), Processed_at (timestamp)
// ============================================================================
const PAYMENT_STATUS = {
  SUCCESS: 'SUCCESS',
  FAILED: 'FAILED',
};

let currentPaymentId = 1;
const paymentDb = new Map(); // order_id -> Payment

async function simulateCharge() {
  if (faultConfig.delay_ms > 0) {
    await sleep(faultConfig.delay_ms);
  }
  if (Math.random() < faultConfig.error_rate) {
    return { ok: false, httpStatus: 500, reason: 'Falha simulada no processador de pagamentos.' };
  }
  return { ok: true };
}

// ============================================================================
// ENDPOINTS HTTP
// ============================================================================

// POST /payments – inicia o processo de pagamento
app.post('/payments', async (req, res) => {
  const { order_id, amount } = req.body || {};

  // Validação dos dados recebidos
  if (!Number.isInteger(order_id) || order_id <= 0) {
    return res.status(400).json({ error: 'order_id tem de ser um inteiro positivo.' });
  }
  if (typeof amount !== 'number' || !Number.isFinite(amount) || amount < 0) {
    return res.status(400).json({ error: 'amount tem de ser um valor numerico >= 0.' });
  }

  // Idempotência: caso o Order realize retries de uma encomenda já aprovada
  const existing = paymentDb.get(order_id);
  if (existing && existing.status === PAYMENT_STATUS.SUCCESS) {
    return res.status(200).json({ message: 'Pagamento ja processado com sucesso.', payment: existing });
  }

  const payment = {
    id: currentPaymentId++,
    order_id,
    amount: Math.round(amount * 100) / 100,
    status: null,
    processed_at: null,
  };

  const charge = await simulateCharge();
  payment.processed_at = new Date().toISOString();

  if (charge.ok) {
    payment.status = PAYMENT_STATUS.SUCCESS;
    paymentDb.set(order_id, payment);
    return res.status(201).json({ message: 'Pagamento aprovado.', payment });
  }

  payment.status = PAYMENT_STATUS.FAILED;
  paymentDb.set(order_id, payment);
  return res.status(charge.httpStatus).json({ error: charge.reason, payment });
});

// POST /payments/configure-faults – permite configurar taxa de erros e latência
app.post('/payments/configure-faults', (req, res) => {
  const { error_rate, delay_ms } = req.body || {};

  const isRate = (v) => typeof v === 'number' && v >= 0 && v <= 1;

  if (error_rate !== undefined && !isRate(error_rate)) {
    return res.status(400).json({ error: 'error_rate tem de ser um numero entre 0 e 1.' });
  }
  if (delay_ms !== undefined && (!Number.isInteger(delay_ms) || delay_ms < 0)) {
    return res.status(400).json({ error: 'delay_ms tem de ser um inteiro >= 0.' });
  }

  if (error_rate !== undefined) faultConfig.error_rate = error_rate;
  if (delay_ms !== undefined) faultConfig.delay_ms = delay_ms;

  console.log('Payment: Configuracao de falhas atualizada', faultConfig);
  return res.status(200).json({ message: 'Configuracao de falhas atualizada.', faults: faultConfig });
});

// GET /payments/:order_id – consulta do estado do pagamento de uma encomenda
app.get('/payments/:order_id', (req, res) => {
  const orderId = parseInt(req.params.order_id, 10);
  const payment = paymentDb.get(orderId);
  if (!payment) {
    return res.status(404).json({ error: 'Pagamento nao encontrado para a encomenda indicada.' });
  }
  return res.json(payment);
});

// GET /metrics – expõe as métricas para o Prometheus
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', client.register.contentType);
  res.end(await client.register.metrics());
});

app.listen(PORT, () => {
  console.log(`Payment Service a correr na porta ${PORT}`);
});
