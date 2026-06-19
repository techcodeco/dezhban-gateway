import { redis } from "../config/redis.js";

const instanceId = parseInt(process.env.NODE_APP_INSTANCE || "0", 10);
const basePort = parseInt(process.env.PORT || "3000", 10);
const PORT = basePort + instanceId;

export const health = () => ({
  ok: true,
  service: "gateway",
  developer: "techcode",
  redisConnected: redis.status === "ready",
  port: PORT,
});
