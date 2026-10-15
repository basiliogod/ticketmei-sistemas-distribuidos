const express = require('express');
const client = require('prom-client');

const app = express();
const PORT = 8000;
const CATALOG_URL = process.env.CATALOG_URL || 'http://catalog:8001';

// Métricas pedidas no eneunciado
const gatewayRequestsTotal = new client.Counter({
  name: 'gateway_requests_total',
  help: 'Total de pedidos recebidos pelo gateway',
  labelNames: ['method', 'endpoint', 'http_status'],
});

const gatewayRequestDurationSeconds = new client.Histogram({
  name: 'gateway_request_duration_seconds',
  help: 'Duração dos pedidos do gateway em segundos',
  labelNames: ['endpoint'],
  buckets: [0.01, 0.05, 0.1, 0.2, 0.5, 1, 2],
});

// Middleware de métricas
app.use((req, res, next) => {
  if (req.path === '/metrics') return next();
  const endTimer = gatewayRequestDurationSeconds.startTimer({
    endpoint: req.path,
  });
  res.on('finish', () => {
    endTimer();
    gatewayRequestsTotal.inc({
      method: req.method,
      endpoint: req.route ? req.route.path : req.path,
      http_status: res.statusCode.toString(),
    });
  });
  next();
});

// Encaminhamento síncrono para o Catalog
app.get('/events', async (req, res) => {
  try {
    const response = await fetch(`${CATALOG_URL}/events`);
    const data = await response.json();
    res.status(response.status).json(data);
  } catch (error) {
    res.status(502).json({ error: 'Erro de comunicação com o catalog' });
  }
});

app.get('/events/:id', async (req, res) => {
  try {
    const response = await fetch(`${CATALOG_URL}/events/${req.params.id}`);
    const data = await response.json();
    res.status(response.status).json(data);
  } catch (error) {
    res.status(502).json({ error: 'Erro de comunicação com o catalog' });
  }
});

// Endpoint de métricas para o Prometheus
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', client.register.contentType);
  res.end(await client.register.metrics());
});

// Endpoint raiz para verificar se o serviço está ativo
app.get('/', (req, res) => {
  res.send('Serviço ativo! Consulta os endpoints em /events ou /metrics.');
});

app.listen(PORT, () => {
  console.log(`API Gateway a ser executado na porta ${PORT}`);
});