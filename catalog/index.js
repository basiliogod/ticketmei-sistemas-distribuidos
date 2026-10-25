const express = require('express');
const fs = require('fs');
const path = require('path');
const client = require('prom-client');

const app = express();
const PORT = 8001;

// Métricas pedidas no enunciado
const catalogRequestsTotal = new client.Counter({
  name: 'catalog_requests_total',
  help: 'Total de pedidos recebidos pelo Catalog',
  labelNames: ['endpoint', 'http_status'],
});

const catalogRequestDurationSeconds = new client.Histogram({
  name: 'catalog_request_duration_seconds',
  help: 'Duração dos pedidos do Catalog em segundos',
  buckets: [0.01, 0.05, 0.1, 0.2, 0.5, 1],
});

// Middleware de métricas
app.use((req, res, next) => {
  if (req.path === '/metrics') return next();
  const endTimer = catalogRequestDurationSeconds.startTimer();
  res.on('finish', () => {
    endTimer();
    catalogRequestsTotal.inc({
      endpoint: req.route ? req.route.path : req.path,
      http_status: res.statusCode.toString(),
    });
  });
  next();
});

// Ler eventos.json
const eventsPath = path.join(__dirname, 'data', 'events.json');
const getEvents = () => JSON.parse(fs.readFileSync(eventsPath, 'utf8'));

// Endpoints
app.get('/events', (req, res) => {
  res.json(getEvents());
});

app.get('/events/:id', (req, res) => {
  const events = getEvents();
  const event = events.find((e) => e.id === req.params.id);
  if (!event) return res.status(404).json({ error: 'Evento não encontrado' });
  res.json(event);
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
  console.log(`Catalog a ser executado na porta ${PORT}`);
});
