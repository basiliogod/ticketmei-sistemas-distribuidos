const express = require('express');
const client = require('prom-client');
const Redis = require('ioredis');

client.collectDefaultMetrics();

const app = express();
app.use(express.json());

const PORT = 8000;
const CATALOG_URL = process.env.CATALOG_URL || 'http://catalog:8001';
const ORDER_URL = process.env.ORDER_URL || 'http://order:8002';
const REDIS_URL = process.env.REDIS_URL || 'redis://redis:6379';

// Configuração do cliente Redis para o Rate Limiter
const redisClient = new Redis(REDIS_URL, {
  maxRetriesPerRequest: 3,
  retryStrategy(times) {
    return Math.min(times * 50, 2000);
  }
});

redisClient.on('connect', () => console.log('Ligado com sucesso ao Redis!'));
redisClient.on('error', (err) => console.error('Erro na ligação ao Redis:', err.message));

const WINDOW_SIZE_IN_SECONDS = 180; // Janela de 3 minutos
const MAX_REQUESTS = 100;          // Limite máximo de requisições por IP

// Middleware do Rate Limiter com Redis
const rateLimiter = async (req, res, next) => {
  // Ignorar o endpoint de métricas para o Prometheus não ser limitado
  if (req.path === '/metrics' || req.path === '/') return next();

  try {
    const ip = req.ip || req.headers['x-forwarded-for'] || 'unknown';
    const key = `ratelimit:${ip}`;

    // Incrementa de forma atômica no Redis
    const requests = await redisClient.incr(key);

    if (requests === 1) {
      await redisClient.expire(key, WINDOW_SIZE_IN_SECONDS);
    }

    const ttl = await redisClient.ttl(key);

    // Cabeçalhos informativos padrão da indústria
    res.setHeader('X-RateLimit-Limit', MAX_REQUESTS);
    res.setHeader('X-RateLimit-Remaining', Math.max(0, MAX_REQUESTS - requests));

    if (requests > MAX_REQUESTS) {
      return res.status(429).json({
        error: 'Too Many Requests',
        message: 'Limite de requisições excedido. Tente novamente mais tarde.',
        retryAfterSeconds: ttl,
      });
    }

    next();
  } catch (error) {
    console.error('Erro no Rate Limiter (Redis):', error);
    next();
  }
};


// Métricas pedidas no enunciado
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

// Aplicar o Rate Limiter globalmente
app.use(rateLimiter);

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

// Encaminhamento síncrono para o Order
app.post('/orders', async (req, res) => {
  try {
    const response = await fetch(`${ORDER_URL}/orders`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req.body),
    });
    const data = await response.json();
    res.status(response.status).json(data);
  } catch (error) {
    res.status(502).json({ error: 'Erro de comunicacao com o Order' });
  }
});

app.get('/orders/:id', async (req, res) => {
  try {
    const response = await fetch(`${ORDER_URL}/orders/${req.params.id}`);
    const data = await response.json();
    res.status(response.status).json(data);
  } catch (error) {
    res.status(502).json({ error: 'Erro de comunicacao com o Order' });
  }
});

// Endpoint de métricas para o Prometheus
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', client.register.contentType);
  res.end(await client.register.metrics());
});

// Endpoint raiz para verificar se o serviço está ativo
app.get('/', (req, res) => {
  res.send('Serviço ativo! Consulta os endpoints em <a href="/events">/events</a>, <a href="/metrics">/metrics</a> ou <a href="/orders">/orders</a>.');
});

app.listen(PORT, () => {
  console.log(`API Gateway a ser executado na porta ${PORT}`);
});
