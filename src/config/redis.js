import Redis from "ioredis";

const redis = new Redis(process.env.REDIS_URI, {
  enableReadyCheck: false,
  //lazyConnect: true,
  maxRetriesPerRequest: 1,
  retryStrategy: (times) => {
    if (times > 3) return null;
    return Math.min(times * 100, 500);
  },
  connectTimeout: 5000,
  commandTimeout: 3000,
  keepAlive: 30000,
  family: 4,
  showFriendlyErrorStack: false,
  autoResendUnfulfilledCommands: false,
  enableAutoPipelining: true,
  enableOfflineQueue: true,
  noDelay: true,
  tls: false,
});

redis.on("connect", () =>
  console.log("connection to redis database successed"),
);
redis.on("error", (err) => console.error("Redis error:", err));

export { redis };
