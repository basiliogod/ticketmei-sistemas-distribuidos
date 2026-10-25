
const amqp = require("amqplib");
const http = require("http");
const client = require("prom-client");

// -----------------------------------------------------------------------------
// Configuration
// -----------------------------------------------------------------------------

const PORT = Number(process.env.PORT || 8003);

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672";

const QUEUE_NAME = process.env.QUEUE_NAME || 'order_notifications';

console.log("Queue Name:")
console.log(QUEUE_NAME)

const PREFETCH_COUNT = Number(process.env.PREFETCH_COUNT || 1);
const RECONNECT_DELAY_MS = Number(
  process.env.RECONNECT_DELAY_MS || 5000
);

// -----------------------------------------------------------------------------
// Virtual notification database
// -----------------------------------------------------------------------------

const notificationDb = new Map();
let currentNotificationId = 1;

/**
 * Creates and stores a notification record.
 *
 * Notification:
 * {
 *   id: integer,
 *   order_id: integer,
 *   email: string,
 *   status: "SENT" | "FAILED"
 * }
 */
function createNotification(order_id, email, status) {
  const notificationId = currentNotificationId++;

  const notification = {
    id: notificationId,
    order_id: Number(order_id),
    email,
    status,
  };

  notificationDb.set(notificationId, notification);

  console.log("Notification saved to notification_db:", notification);

  return notification;
}

// -----------------------------------------------------------------------------
// Prometheus metrics
// -----------------------------------------------------------------------------

const register = new client.Registry();

client.collectDefaultMetrics({ register });

const notificationsProcessed = new client.Counter({
  name: "notifications_processed_total",
  help: "Total number of notification events processed.",
  labelNames: ["status"],
  registers: [register],
});

// Explicitly initialize both labeled time series.
notificationsProcessed.labels("success").inc(0);
notificationsProcessed.labels("failure").inc(0);

const notificationProcessingDuration = new client.Histogram({
  name: "notification_processing_duration_seconds",
  help: "Time spent processing notification events in seconds.",
  buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  registers: [register],
});

// -----------------------------------------------------------------------------
// Simulated email sender
// -----------------------------------------------------------------------------

async function sendEmail(email, subject, body) {
  // Simulate sending an email without an external email provider.
  console.log("--------------------------------------------------");
  console.log("SIMULATED EMAIL");
  console.log("To:", email);
  console.log("Subject:", subject);
  console.log("Body:", body);
  console.log("Email sent successfully (simulated).");
  console.log("--------------------------------------------------");

  return true;
}

// -----------------------------------------------------------------------------
// Event processing
// -----------------------------------------------------------------------------

async function processMessage(messagePayload) {
  const { event } = messagePayload;

  switch (event) {
    case "OrderCreated": {
      const {
        order_id,
        customer_id,
        email,
        total_amount,
      } = messagePayload;

      const recipient =
        email || `customer_${customer_id}@ticketmei.com`;

      console.log("Handling OrderCreated event:", messagePayload);

      try {
        const subject = `Order #${order_id} Confirmation`;

        const body = [
          `Hello Customer ${customer_id},`,
          "",
          `Your order #${order_id} has been created successfully.`,
          `Total amount: ${total_amount}`,
          "",
          "Thank you for your purchase!",
        ].join("\n");

        await sendEmail(recipient, subject, body);

        createNotification(order_id, recipient, "SENT");
      } catch (error) {
        createNotification(order_id, recipient, "FAILED");
        throw error;
      }

      break;
    }

    default:
      console.warn(`Unsupported event received: "${event}"`);
      throw new Error(`Unsupported event: ${event}`);
  }
}

// -----------------------------------------------------------------------------
// HTTP server: Prometheus metrics and health endpoint
// -----------------------------------------------------------------------------

const server = http.createServer(async (req, res) => {
  const pathname = new URL(
    req.url,
    `http://${req.headers.host || "localhost"}`
  ).pathname;

  if (req.method === "GET" && pathname === "/metrics") {
    try {
      res.writeHead(200, {
        "Content-Type": register.contentType,
      });

      res.end(await register.metrics());
    } catch (error) {
      console.error("Failed to collect metrics:", error);

      res.writeHead(500);
      res.end("Failed to collect metrics");
    }

    return;
  }

  if (req.method === "GET" && pathname === "/health") {
    res.writeHead(200, {
      "Content-Type": "application/json",
    });

    res.end(JSON.stringify({
      status: "ok",
      service: "notification",
    }));

    return;
  }

  res.writeHead(404, {
    "Content-Type": "application/json",
  });

  res.end(JSON.stringify({ error: "Not found" }));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Notification service listening on port ${PORT}`);
});

// -----------------------------------------------------------------------------
// RabbitMQ consumer
// -----------------------------------------------------------------------------

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

let shuttingDown = false;
let currentConnection = null;
let currentChannel = null;

async function startWorker() {
  while (!shuttingDown) {
    let connection = null;

    try {
      console.log("Connecting to RabbitMQ...");

      connection = await amqp.connect(RABBITMQ_URL);
      currentConnection = connection;

      connection.on("error", (error) => {
        console.error("RabbitMQ connection error:", error.message);
      });

      const channel = await connection.createChannel();
      currentChannel = channel;

      await channel.assertQueue(QUEUE_NAME, {
        durable: true,
      });

      await channel.prefetch(PREFETCH_COUNT);

      console.log(`Listening on queue "${QUEUE_NAME}"`);

      await channel.consume(
        QUEUE_NAME,
        async (msg) => {
          if (msg === null) {
            console.warn("RabbitMQ consumer was cancelled.");
            return;
          }

          const endTimer =
            notificationProcessingDuration.startTimer();

          let status = "failure";

          try {
            const messagePayload = JSON.parse(
              msg.content.toString("utf8")
            );

            await processMessage(messagePayload);

            channel.ack(msg);
            status = "success";
          } catch (error) {
            console.error("Notification processing failed:", error);

            try {
              // Requeue failed messages.
              // Configure retry limits and a dead-letter queue
              // to prevent infinite retry loops in production.
              channel.nack(msg, false, true);
            } catch (nackError) {
              console.error("Failed to reject message:", nackError);
            }
          } finally {
            notificationsProcessed.labels(status).inc();
            endTimer();
          }
        },
        { noAck: false }
      );

      await new Promise((resolve) => {
        connection.once("close", resolve);
      });
    } catch (error) {
      console.error("RabbitMQ worker error:", error.message);
    } finally {
      if (currentConnection === connection) {
        currentConnection = null;
        currentChannel = null;
      }
    }

    if (!shuttingDown) {
      console.log(
        `Reconnecting to RabbitMQ in ${RECONNECT_DELAY_MS}ms...`
      );

      await sleep(RECONNECT_DELAY_MS);
    }
  }
}

// -----------------------------------------------------------------------------
// Graceful shutdown
// -----------------------------------------------------------------------------

async function shutdown(signal) {
  if (shuttingDown) return;

  shuttingDown = true;

  console.log(`Received ${signal}; shutting down...`);

  if (currentChannel) {
    try {
      await currentChannel.close();
    } catch (error) {
      console.error("Error closing RabbitMQ channel:", error.message);
    }
  }

  if (currentConnection) {
    try {
      await currentConnection.close();
    } catch (error) {
      console.error("Error closing RabbitMQ connection:", error.message);
    }
  }

  server.close(() => {
    console.log("Notification HTTP server closed.");
  });
}

process.once("SIGINT", () => shutdown("SIGINT"));
process.once("SIGTERM", () => shutdown("SIGTERM"));

startWorker().catch((error) => {
  console.error("Fatal worker error:", error);
  process.exitCode = 1;
});
