const express = require('express');
const amqp = require('amqplib');
const client = require('prom-client');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 8002;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@rabbitmq:5672';
const QUEUE_NAME = 'order_notifications';

// Métrica exigida para o Prometheus
const ordersInitiatedTotal = new client.Counter({
  name: 'orders_initiated_total',
  help: 'Total de encomendas iniciadas no servico Order',
  labelNames: ['status'],
});

// Base de dados em memória para a Fase 2 (order_db)
const ordersDb = new Map();
let currentOrderId = 1;
let rabbitChannel = null;

// Conexão resiliente ao RabbitMQ
async function connectRabbitMQ() {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    rabbitChannel = await connection.createChannel();
    await rabbitChannel.assertQueue(QUEUE_NAME, { durable: true });
    console.log('Order: Conectado ao RabbitMQ');
  } catch (err) {
    console.error('Order: Erro a conectar ao RabbitMQ, tentando em 3s...', err.message);
    setTimeout(connectRabbitMQ, 3000);
  }
}
connectRabbitMQ();

// POST /orders
app.post('/orders', (req, res) => {
  const { customer_id, event_id, total_amount, quantity, email } = req.body;

  // Validação do limite de bilhetes por compra (RF2: ex. max 4)
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
    status: 'PENDING'
  };

  ordersDb.set(orderId, order);
  ordersInitiatedTotal.inc({ status: 'PENDING' });

  // Evento assíncrono para o Notification Service via RabbitMQ
  const messagePayload = {
    event: 'OrderCreated',
    order_id: order.id,
    customer_id: order.customer_id,
    email: email || `customer_${order.customer_id}@ticketmei.com`,
    total_amount: order.total_amount
  };

  if (rabbitChannel) {
	  
	console.log("SENDING RABBITMQ MESSAGE")
	console.log("ON QUEUE:")
	console.log(QUEUE_NAME)
	  
    rabbitChannel.sendToQueue(
      QUEUE_NAME,
      Buffer.from(JSON.stringify(messagePayload)),
      { persistent: true }
    );
  } else {
    console.warn('Order: Canal RabbitMQ indisponivel no momento.');
  }

  // Devolve HTTP 202 Accepted conforme o requisito da Fase 2
  return res.status(202).json({
    message: 'Pedido de compra aceite e a ser processado.',
    order: order
  });
});

// GET /orders/:id
app.get('/orders/:id', (req, res) => {
  const id = parseInt(req.params.id, 10);
  const order = ordersDb.get(id);

  if (!order) {
    return res.status(404).json({ error: 'Order nao encontrada.' });
  }

  return res.json(order);
});

// GET /metrics
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', client.register.contentType);
  res.end(await client.register.metrics());
});

app.listen(PORT, () => {
  console.log(`Order Service a correr na porta ${PORT}`);
});