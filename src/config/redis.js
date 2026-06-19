import Redis from "ioredis";

const redis = new Redis(process.env.REDIS_URI, {
  enableReadyCheck: false,
  lazyConnect: true,
  maxRetriesPerRequest: 1,
  retryStrategy: (times) => {
    if (times > 3) return null;
    return Math.min(times * 100, 500);
  },

  // تنظیمات شبکه
  connectTimeout: 5000,
  commandTimeout: 3000,
  keepAlive: 30000,
  family: 4,

  // غیرفعال کردن ویژگی‌های سنگین
  showFriendlyErrorStack: false,
  autoResendUnfulfilledCommands: false,
  enableAutoPipelining: true,
  enableOfflineQueue: true,

  // تنظیمات TCP
  noDelay: true,
  tls: false,
});

redis.on("connect", () =>
  console.log("✅ Redis connected via ioredis (RESP2)"),
);
redis.on("error", (err) => console.error("Redis error:", err));

export { redis };
