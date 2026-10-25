
const amqp = require("amqplib");
const http = require("http");
const client = require("prom-client");

// Configuration
const RABBITMQ_URL =
  process.env.RABBITMQ_URL || "amqp://guest:guest@rabbitmq:5672";

const QUEUE_NAME = process.env.QUEUE_NAME || "jobs";
const PREFETCH_COUNT = Number(process.env.PREFETCH_COUNT || 1);
const RECONNECT_DELAY_MS = Number(
  process.env.RECONNECT_DELAY_MS || 5000
);
const PORT = Number(process.env.PORT || 8003);

// -----------------------------------------------------------------------------
// Prometheus metrics
// -----------------------------------------------------------------------------

const register = new client.Registry();

client.collectDefaultMetrics({ register });

const notificationsProcessed = new client.Counter({
  name: "notifications_processed_total",
  help: "Total number of notifications processed.",
  labelNames: ["status"],
  registers: [register],
});

const notificationProcessingDuration = new client.Histogram({
  name: "notification_processing_duration_seconds",
  help: "Time spent processing notifications in seconds.",
  buckets: [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
  registers: [register],
});

// -----------------------------------------------------------------------------
// HTTP server
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
      console.error("Failed to collect Prometheus metrics:", error);

      res.writeHead(500, {
        "Content-Type": "text/plain; charset=utf-8",
      });
      res.end("Failed to collect metrics");
    }

    return;
  }

  if (req.method === "GET" && pathname === "/health") {
    res.writeHead(200, {
      "Content-Type": "application/json",
    });
    res.end(JSON.stringify({ status: "ok" }));
    return;
  }

  res.writeHead(404, {
    "Content-Type": "application/json",
  });
  res.end(JSON.stringify({ error: "Not found" }));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`HTTP server listening on port ${PORT}`);
});

// -----------------------------------------------------------------------------
// Notification processing
// -----------------------------------------------------------------------------

async function processMessage(message) {
  console.log("Processing notification:", message);

  // TODO: Implement your actual business logic here.
  //
  // Examples:
  // - Send an email or push notification.
  // - Call an external API.
  // - Update a database.
  // - Trigger another service or workflow.

  console.log("Notification processed successfully.");
}

// -----------------------------------------------------------------------------
// RabbitMQ consumer
// -----------------------------------------------------------------------------

const sleep = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

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

          const endTimer = notificationProcessingDuration.startTimer();
          let status = "failure";

          try {
            const message = JSON.parse(msg.content.toString("utf8"));

            await processMessage(message);

            // Acknowledge only after successful processing.
            channel.ack(msg);
            status = "success";
          } catch (error) {
            console.error("Notification processing failed:", error);

            // Requeue failed deliveries. Configure a retry limit and
            // dead-letter queue to avoid infinite retry loops.
            try {
              channel.nack(msg, false, true);
            } catch (nackError) {
              console.error("Failed to reject RabbitMQ message:", nackError);
            }
          } finally {
            notificationsProcessed.inc({ status });
            endTimer();
          }
        },
        { noAck: false }
      );

      // Wait for the connection to close before reconnecting.
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
      console.log(`Reconnecting to RabbitMQ in ${RECONNECT_DELAY_MS}ms...`);
      await sleep(RECONNECT_DELAY_MS);
    }
  }
}

// -----------------------------------------------------------------------------
// Graceful shutdown
// -----------------------------------------------------------------------------

async function shutdown(signal) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  console.log(`Received ${signal}; shutting down...`);

  // Closing the channel stops the consumer and requeues unacknowledged
  // deliveries, subject to RabbitMQ's normal channel-close behavior.
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
    console.log("HTTP server closed.");
  });
}

process.once("SIGINT", () => shutdown("SIGINT"));
process.once("SIGTERM", () => shutdown("SIGTERM"));

startWorker().catch((error) => {
  console.error("Fatal worker error:", error);
  process.exitCode = 1;
});
