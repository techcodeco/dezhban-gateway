// workers/production-worker.js
import Redis from "ioredis";
import crypto from "crypto";

// ===== تنظیمات محیطی با اعتبارسنجی =====
const REDIS_CONFIG = {
  host: process.env.REDIS_HOST || "localhost",
  port: parseInt(process.env.REDIS_PORT || "6379"),
  password: process.env.REDIS_PASSWORD || undefined,
  maxRetriesPerRequest: 3,
  retryStrategy: (times) => {
    if (times > 10) {
      console.error(`❌ Redis connection failed after ${times} attempts`);
      process.exit(1);
    }
    const delay = Math.min(times * 100, 3000);
    return delay;
  },
  enableReadyCheck: true,
  enableOfflineQueue: true,
  commandTimeout: 5000,
  keepAlive: 30000,
};

const STREAM_NAME = process.env.STREAM_NAME || "rubika:updates";
const GROUP_NAME = process.env.GROUP_NAME || "rubika-workers";
const CONSUMER_NAME = `worker-${process.pid}-${crypto.randomBytes(4).toString("hex")}`;

// ===== تنظیمات پردازش =====
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "100");
const BLOCK_MS = parseInt(process.env.BLOCK_MS || "1000");
const MAX_CONCURRENT = parseInt(process.env.MAX_CONCURRENT || "50");
const PROCESSING_TIMEOUT = parseInt(process.env.PROCESSING_TIMEOUT || "30000");
const DEAD_LETTER_QUEUE = process.env.DEAD_LETTER_QUEUE || "rubika:dead-letter";

// ===== اتصال به Redis با تنظیمات پیشرفته =====
console.log("🔌 Connecting to Redis...");
const redis = new Redis(REDIS_CONFIG);

// مانیتورینگ اتصال
redis.on("connect", () => console.log("✅ Redis connected"));
redis.on("ready", () => console.log("✅ Redis ready"));
redis.on("reconnecting", () => console.log("🔄 Redis reconnecting..."));
redis.on("error", (err) => console.error("❌ Redis error:", err.message));

// ===== ابزارهای کمکی =====
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

class MetricsCollector {
  constructor() {
    this.processed = 0;
    this.errors = 0;
    this.retries = 0;
    this.processingTime = [];
    this.startTime = Date.now();
  }

  recordSuccess(duration) {
    this.processed++;
    this.processingTime.push(duration);
    if (this.processingTime.length > 1000) this.processingTime.shift();
  }

  recordError() {
    this.errors++;
  }

  recordRetry() {
    this.retries++;
  }

  getStats() {
    const elapsed = (Date.now() - this.startTime) / 1000;
    const rpm = (this.processed / elapsed) * 60;
    const avgProcessingTime =
      this.processingTime.length > 0
        ? this.processingTime.reduce((a, b) => a + b, 0) /
          this.processingTime.length
        : 0;

    return {
      processed: this.processed,
      errors: this.errors,
      retries: this.retries,
      uptime: elapsed,
      rpm: Math.round(rpm),
      avgProcessingTime: avgProcessingTime.toFixed(2),
    };
  }
}

const metrics = new MetricsCollector();

// ===== تنظیم Stream و Consumer Group =====
async function setupStreamAndGroup() {
  console.log("\n🔧 Setting up stream and consumer group...");

  try {
    await redis.ping();

    // ایجاد Stream (در صورت نیاز)
    const streamExists = await redis.exists(STREAM_NAME);
    if (!streamExists) {
      console.log(`📝 Creating stream: ${STREAM_NAME}`);
      await redis.xadd(STREAM_NAME, "*", "init", Date.now().toString());
      console.log("✅ Stream created");
    }

    // ایجاد Consumer Group
    try {
      await redis.xgroup("CREATE", STREAM_NAME, GROUP_NAME, "0", "MKSTREAM");
      console.log("✅ Consumer group created");
    } catch (err) {
      if (!err.message?.includes("BUSYGROUP")) throw err;
      console.log("✅ Consumer group already exists");
    }

    // بازیابی پیام‌های معلق
    const pending = await redis.xpending(
      STREAM_NAME,
      GROUP_NAME,
      "-",
      "+",
      100,
    );
    if (pending && pending.length > 0) {
      console.log(`⚠️ Found ${pending.length} pending messages, will retry...`);
    }

    console.log("✅ Setup complete!\n");
    return true;
  } catch (err) {
    console.error("❌ Setup failed:", err);
    return false;
  }
}

// ===== پردازش پیام با timeout و retry =====
async function processMessageWithTimeout(
  id,
  data,
  timeoutMs = PROCESSING_TIMEOUT,
) {
  return Promise.race([
    processMessage(id, data),
    new Promise((_, reject) =>
      setTimeout(
        () => reject(new Error(`Processing timeout after ${timeoutMs}ms`)),
        timeoutMs,
      ),
    ),
  ]);
}

async function processMessage(id, data) {
  // ========== کد پردازش واقعی شما اینجاست ==========
  // مثال: ذخیره در دیتابیس، ارسال به API، و غیره

  try {
    let payload;
    if (typeof data === "string") {
      payload = JSON.parse(data);
    } else if (data.data) {
      payload = JSON.parse(data.data);
    } else {
      payload = data;
    }

    // پردازش بر اساس نوع آپدیت
    if (payload.update) {
      const update = payload.update;

      if (update.message) {
        // پردازش پیام
        console.log(`  💬 [${id}] Message: ${update.message.substring(0, 50)}`);
        // await messageHandler(update);
      }

      if (update.callback_query) {
        // پردازش callback
        console.log(`  🔘 [${id}] Callback: ${update.callback_query.data}`);
        // await callbackHandler(update);
      }

      if (update.inline_query) {
        // پردازش inline query
        console.log(`  🔍 [${id}] Inline query: ${update.inline_query.query}`);
        // await inlineHandler(update);
      }
    }

    return { success: true };
  } catch (err) {
    console.error(`  ❌ [${id}] Processing error:`, err.message);
    return { success: false, error: err.message };
  }
}

// ===== ذخیره در Dead Letter Queue =====
async function saveToDeadLetter(id, fields, error) {
  try {
    await redis.xadd(
      DEAD_LETTER_QUEUE,
      "*",
      "original_id",
      id,
      "original_stream",
      STREAM_NAME,
      "data",
      typeof fields === "string" ? fields : JSON.stringify(fields),
      "error",
      error,
      "timestamp",
      Date.now().toString(),
      "consumer",
      CONSUMER_NAME,
    );
    console.log(`  📋 Saved to dead letter queue: ${id}`);
  } catch (err) {
    console.error(`  ❌ Failed to save to DLQ:`, err.message);
  }
}

// ===== Worker اصلی با پردازش موازی =====
class ProductionWorker {
  constructor() {
    this.isRunning = true;
    this.activeProcesses = new Map();
    this.statsInterval = null;
  }

  async start() {
    console.log("🚀 Starting Production Redis Stream Worker\n");
    console.log(`📋 Configuration:
  Stream: ${STREAM_NAME}
  Group: ${GROUP_NAME}
  Consumer: ${CONSUMER_NAME}
  Redis: ${REDIS_CONFIG.host}:${REDIS_CONFIG.port}
  Batch Size: ${BATCH_SIZE}
  Max Concurrent: ${MAX_CONCURRENT}
  Processing Timeout: ${PROCESSING_TIMEOUT}ms
`);

    const ready = await setupStreamAndGroup();
    if (!ready) {
      console.error("Fatal: Could not setup stream/group");
      process.exit(1);
    }

    console.log(`👂 Listening for messages...\n`);
    this.startStatsReporter();
    await this.processLoop();
  }

  startStatsReporter() {
    this.statsInterval = setInterval(() => {
      const stats = metrics.getStats();
      console.log(`
╔════════════════════════════════════════════════════════╗
║              Worker Statistics                        ║
╠════════════════════════════════════════════════════════╣
║  Processed:   ${stats.processed.toString().padEnd(20)}║
║  Errors:      ${stats.errors.toString().padEnd(20)}║
║  Retries:     ${stats.retries.toString().padEnd(20)}║
║  RPM:         ${stats.rpm.toString().padEnd(20)}║
║  Avg Time:    ${stats.avgProcessingTime}ms${" ".repeat(20 - stats.avgProcessingTime.length)}║
║  Uptime:      ${stats.uptime.toFixed(0)}s${" ".repeat(20 - stats.uptime.toFixed(0).length)}║
║  Active:      ${this.activeProcesses.size.toString().padEnd(20)}║
╚════════════════════════════════════════════════════════╝
      `);
    }, 30000);
  }

  async processLoop() {
    while (this.isRunning) {
      try {
        // کنترل همزمانی
        if (this.activeProcesses.size >= MAX_CONCURRENT) {
          await sleep(10);
          continue;
        }

        const availableSlots = MAX_CONCURRENT - this.activeProcesses.size;
        const count = Math.min(BATCH_SIZE, availableSlots);

        const results = await redis.xreadgroup(
          "GROUP",
          GROUP_NAME,
          CONSUMER_NAME,
          "BLOCK",
          BLOCK_MS,
          "COUNT",
          count,
          "STREAMS",
          STREAM_NAME,
          ">",
        );

        if (!results || results.length === 0) continue;

        for (const result of results) {
          const entries = result[1];

          for (const entry of entries) {
            const id = entry[0];
            const fields = entry[1];

            // پردازش غیرمسدود
            this.processAsync(id, fields);
          }
        }
      } catch (err) {
        console.error("\n❌ Worker error:", err.message);

        if (err.message?.includes("NOGROUP")) {
          console.log("🔄 Consumer group missing, reinitializing...");
          await setupStreamAndGroup();
        }

        await sleep(1000);
      }
    }
  }

  async processAsync(id, fields) {
    const startTime = Date.now();
    this.activeProcesses.set(id, startTime);

    // پردازش در setImmediate برای عدم阻塞
    setImmediate(async () => {
      try {
        const result = await processMessageWithTimeout(id, fields);

        if (result.success) {
          await redis.xack(STREAM_NAME, GROUP_NAME, id);
          metrics.recordSuccess(Date.now() - startTime);
          console.log(`  ✅ [${id}] Processed (${Date.now() - startTime}ms)`);
        } else {
          // تلاش مجدد برای خطاهای موقتی
          const retryCount = await this.getRetryCount(id);
          if (retryCount < 3) {
            metrics.recordRetry();
            console.log(`  🔄 [${id}] Will retry (attempt ${retryCount + 1})`);
          } else {
            await saveToDeadLetter(id, fields, result.error);
            await redis.xack(STREAM_NAME, GROUP_NAME, id);
            metrics.recordError();
            console.log(`  ⚰️ [${id}] Sent to dead letter queue`);
          }
        }
      } catch (err) {
        console.error(`  ❌ [${id}] Fatal error:`, err.message);
        await saveToDeadLetter(id, fields, err.message);
        await redis.xack(STREAM_NAME, GROUP_NAME, id);
        metrics.recordError();
      } finally {
        this.activeProcesses.delete(id);
      }
    });
  }

  async getRetryCount(id) {
    try {
      const pending = await redis.xpending(STREAM_NAME, GROUP_NAME, id, id, 1);
      return pending?.[0]?.times || 0;
    } catch {
      return 0;
    }
  }

  async stop() {
    console.log("\n🛑 Stopping worker gracefully...");
    this.isRunning = false;

    if (this.statsInterval) {
      clearInterval(this.statsInterval);
    }

    // منتظر اتمام پردازش‌های فعال
    let waitTime = 0;
    while (this.activeProcesses.size > 0 && waitTime < 30000) {
      console.log(
        `⏳ Waiting for ${this.activeProcesses.size} active processes...`,
      );
      await sleep(1000);
      waitTime += 1000;
    }

    await redis.quit();
    console.log("✅ Worker stopped");
  }
}

// ===== راه‌اندازی Worker =====
const worker = new ProductionWorker();

worker.start().catch(async (err) => {
  console.error("Fatal error:", err);
  await worker.stop();
  process.exit(1);
});

// ===== Graceful Shutdown =====
const shutdown = async (signal) => {
  console.log(`\n🛑 Received ${signal}, shutting down gracefully...`);
  await worker.stop();
  process.exit(0);
};

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));

// ===== Uncaught Error Handling =====
process.on("uncaughtException", async (err) => {
  console.error("Uncaught Exception:", err);
  await worker.stop();
  process.exit(1);
});

process.on("unhandledRejection", async (err) => {
  console.error("Unhandled Rejection:", err);
  await worker.stop();
  process.exit(1);
});
