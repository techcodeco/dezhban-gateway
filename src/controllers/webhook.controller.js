import { redis } from "../config/redis.js";

const STREAM_NAME = process.env.STREAM_NAME || "rubika:updates";
const BATCH_SIZE = 500; // افزایش به 500 برای吞吐 بیشتر
const BATCH_TIMEOUT_MS = 5; // کاهش به 3ms

class UltraFastBatchProcessor {
  constructor(redis, streamName, batchSize = 500, timeoutMs = 3) {
    this.redis = redis;
    this.streamName = streamName;
    this.batchSize = batchSize;
    this.timeoutMs = timeoutMs;
    this.batch = [];
    this.timer = null;
    this.processing = false;
  }

  add(payload) {
    return new Promise((resolve, reject) => {
      this.batch.push({ payload, resolve, reject });
      if (this.batch.length >= this.batchSize) {
        if (this.timer) {
          clearTimeout(this.timer);
          this.timer = null;
        }
        this.flush().catch((err) => console.error("Flush error:", err));
      } else if (!this.timer) {
        this.timer = setTimeout(() => {
          this.timer = null;
          this.flush().catch((err) => console.error("Flush error:", err));
        }, this.timeoutMs);
      }
    });
  }

  addFireAndForget(payload) {
    this.batch.push({ payload });
    if (this.batch.length >= this.batchSize) {
      if (this.timer) {
        clearTimeout(this.timer);
        this.timer = null;
      }
      this.flush().catch((err) => console.error("Flush error:", err));
    } else if (!this.timer) {
      this.timer = setTimeout(() => {
        this.timer = null;
        this.flush().catch((err) => console.error("Flush error:", err));
      }, this.timeoutMs);
    }
  }

  async flush() {
    if (this.processing || this.batch.length === 0) return;
    this.processing = true;
    const currentBatch = [...this.batch];
    this.batch = [];
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    try {
      const pipeline = this.redis.pipeline();
      for (const item of currentBatch) {
        pipeline.xadd(
          this.streamName,
          "MAXLEN",
          "~",
          "10000",
          "*",
          "data",
          typeof item.payload === "string"
            ? item.payload
            : JSON.stringify(item.payload),
        );
      }
      const results = await pipeline.exec();
      for (let i = 0; i < results.length; i++) {
        const [err, result] = results[i];
        const item = currentBatch[i];
        if (err) {
          if (item.reject) item.reject(err);
        } else {
          if (item.resolve) item.resolve(result);
        }
      }
    } catch (error) {
      for (const item of currentBatch) {
        if (item.reject) item.reject(error);
      }
    } finally {
      this.processing = false;
      if (this.batch.length > 0) {
        setImmediate(() => this.flush());
      }
    }
  }

  async close() {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    await this.flush();
  }
}

const batchProcessor = new UltraFastBatchProcessor(
  redis,
  STREAM_NAME,
  BATCH_SIZE,
  BATCH_TIMEOUT_MS,
);

export const webhook = async ({ request, set }) => {
  let jsonPayload;
  try {
    const rawBody = await request.text();
    jsonPayload = JSON.parse(rawBody);
  } catch (error) {
    set.status = 400;
    return { ok: false, message: "Invalid JSON" };
  }
  if (!jsonPayload?.update) {
    set.status = 400;
    return { ok: false, message: "Missing update" };
  }
  try {
    await Promise.race([
      batchProcessor.add(jsonPayload),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error("Queue timeout")), 100),
      ),
    ]);
    set.status = 200;
    return {
      ok: true,
    };
  } catch (error) {
    set.status = 500;
    return { ok: false };
  }
};

const shutdown = async () => {
  console.log("\n🛑 Shutting down gracefully...");
  console.log("⏳ Flushing remaining batches...");
  await batchProcessor.close();
  console.log("✅ All data flushed to Redis");
  await redis.quit();
  console.log("👋 Goodbye!");
  process.exit(0);
};

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
process.on("uncaughtException", (error) => {
  console.error("Uncaught Exception:", error);
  shutdown();
});

process.on("unhandledRejection", (reason, promise) => {
  console.error("Unhandled Rejection at:", promise, "reason:", reason);
});
