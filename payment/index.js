const express = require('express');
const client = require('prom-client');
const inventory = require('./inventory');

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
  // Vai até 15s para cobrir o atraso injetado pelo configure-faults (ex: 10s)
  buckets: [0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15],
});

// Só o POST /payments conta para as métricas de pagamento
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
// INJEÇÃO DE FALHAS (configurável via POST /payments/configure-faults)
// ============================================================================
const faultConfig = {
  error_rate: parseFloat(process.env.FAULT_ERROR_RATE || '0'), // Prob. de erro 500 (0..1)
  delay_ms: parseInt(process.env.FAULT_DELAY_MS || '0', 10), // Latência extra por pagamento
  decline_rate: parseFloat(process.env.DECLINE_RATE || '0'), // Prob. de pagamento recusado (0..1)
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ============================================================================
// BASE DE DADOS EM MEMÓRIA (payment_db)
// Payment: order_id (integer), amount (money), status (enum), processed_at (timestamp)
// ============================================================================
const PAYMENT_STATUS = {
  PENDING: 'PENDING',
  COMPLETED: 'COMPLETED',
  FAILED: 'FAILED',
};

const paymentDb = new Map(); // order_id -> Payment

// Simulação da cobrança. NÃO existe nenhum sistema de pagamento real.
async function simulateCharge() {
  if (faultConfig.delay_ms > 0) {
    await sleep(faultConfig.delay_ms);
  }
  if (Math.random() < faultConfig.error_rate) {
    return { ok: false, httpStatus: 500, reason: 'Falha simulada no processador de pagamentos.' };
  }
  if (Math.random() < faultConfig.decline_rate) {
    return { ok: false, httpStatus: 402, reason: 'Pagamento recusado (simulado).' };
  }
  return { ok: true };
}

// ============================================================================
// ENDPOINTS HTTP
// ============================================================================
app.post('/payments', async (req, res) => {
  const { order_id, amount, event_id = 101, quantity = 1 } = req.body || {};

  // Validação
  if (!Number.isInteger(order_id) || order_id <= 0) {
    return res.status(400).json({ error: 'order_id tem de ser um inteiro positivo.' });
  }
  if (typeof amount !== 'number' || !Number.isFinite(amount) || amount < 0) {
    return res.status(400).json({ error: 'amount tem de ser um valor numerico >= 0.' });
  }
  if (!Number.isInteger(quantity) || quantity < 1 || quantity > 4) {
    return res.status(400).json({ error: 'quantity tem de ser um inteiro entre 1 e 4.' });
  }

  // Idempotência: o Order faz retries com o mesmo order_id
  const existing = paymentDb.get(order_id);
  if (existing && existing.status === PAYMENT_STATUS.COMPLETED) {
    return res.status(200).json({ message: 'Pagamento ja processado.', payment: existing });
  }
  if (existing && existing.status === PAYMENT_STATUS.PENDING) {
    return res.status(409).json({ error: 'Pagamento desta encomenda ja esta a ser processado.' });
  }

  const payment = {
    order_id,
    amount: Math.round(amount * 100) / 100,
    status: PAYMENT_STATUS.PENDING,
    processed_at: null,
  };
  paymentDb.set(order_id, payment);

  // 1. Reservar e decrementar o stock no Inventory ANTES de cobrar
  let reservation;
  try {
    reservation = await inventory.reserve(order_id, event_id, quantity);
  } catch (err) {
    payment.status = PAYMENT_STATUS.FAILED;
    payment.processed_at = new Date().toISOString();
    const statusByCode = { OUT_OF_STOCK: 409, EVENT_NOT_FOUND: 404, UNAVAILABLE: 503 };
    const status = statusByCode[err.code] || 500;
    if (status === 500) console.error('Payment: Erro inesperado ao reservar stock', err);
    return res.status(status).json({ error: err.message, payment });
  }

  // 2. Simular a cobrança
  let charge;
  try {
    charge = await simulateCharge();
  } catch (err) {
    charge = { ok: false, httpStatus: 500, reason: err.message };
  }

  payment.processed_at = new Date().toISOString();

  if (charge.ok) {
    payment.status = PAYMENT_STATUS.COMPLETED;
    return res.status(201).json({ message: 'Pagamento aprovado.', payment });
  }

  // 3. Compensação: devolver os bilhetes reservados ao Inventory
  payment.status = PAYMENT_STATUS.FAILED;
  let compensation = reservation.skipped
    ? 'Sem reserva de stock (Inventory desativado).'
    : 'Bilhetes reservados devolvidos ao inventario.';
  try {
    await inventory.release(reservation);
  } catch (err) {
    compensation = 'Falha ao devolver os bilhetes ao inventario.';
    console.error(`Payment: Compensacao da order ${order_id} falhou`, err.message);
  }
  console.warn(`Payment: Pagamento da order ${order_id} falhou (${charge.reason}). ${compensation}`);

  return res.status(charge.httpStatus).json({ error: charge.reason, compensation, payment });
});

app.post('/payments/configure-faults', (req, res) => {
  const { error_rate, delay_ms, decline_rate } = req.body || {};

  const isRate = (v) => typeof v === 'number' && v >= 0 && v <= 1;

  if (error_rate !== undefined && !isRate(error_rate)) {
    return res.status(400).json({ error: 'error_rate tem de ser um numero entre 0 e 1.' });
  }
  if (decline_rate !== undefined && !isRate(decline_rate)) {
    return res.status(400).json({ error: 'decline_rate tem de ser um numero entre 0 e 1.' });
  }
  if (delay_ms !== undefined && (!Number.isInteger(delay_ms) || delay_ms < 0)) {
    return res.status(400).json({ error: 'delay_ms tem de ser um inteiro >= 0.' });
  }

  if (error_rate !== undefined) faultConfig.error_rate = error_rate;
  if (delay_ms !== undefined) faultConfig.delay_ms = delay_ms;
  if (decline_rate !== undefined) faultConfig.decline_rate = decline_rate;

  console.log('Payment: Configuracao de falhas atualizada', faultConfig);
  return res.status(200).json({ message: 'Configuracao de falhas atualizada.', faults: faultConfig });
});

app.get('/metrics', async (req, res) => {
  res.set('Content-Type', client.register.contentType);
  res.end(await client.register.metrics());
});

app.listen(PORT, () => {
  console.log(`Payment Service a correr na porta ${PORT}`);
  if (!inventory.enabled) {
    console.warn('Payment: INVENTORY_URL nao definido, reservas de stock desativadas.');
  }
});
