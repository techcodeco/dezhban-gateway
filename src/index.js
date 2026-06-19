import dotenv from "dotenv";
dotenv.config();
import { Elysia, t } from "elysia";
import { health } from "./controllers/health.controller.js";
import { webhook } from "./controllers/webhook.controller.js";

const WEBHOOK_SECRET_KEY = process.env.WEBHOOK_SECRET_KEY;

const instanceId = parseInt(process.env.NODE_APP_INSTANCE || "0", 10);
const basePort = parseInt(process.env.PORT || "3000", 10);
const PORT = basePort + instanceId;

if (!WEBHOOK_SECRET_KEY)
  console.warn(
    "[Gateway] WEBHOOK_SECRET_KEY is not set. Webhook verification will be bypassed.",
  );

const app = new Elysia()
  .get("/health", health)
  .post("/webhook/:secret", webhook, {
    params: t.Object({
      secret: t.Literal(WEBHOOK_SECRET_KEY, {
        error: {
          ok: false,
          message: "unauthorized error",
        },
      }),
    }),
  });

try {
  const server = app.listen(PORT, () => {
    console.log(`[Gateway] Instance ${instanceId} is running on port ${PORT}`);
  });
} catch (error) {
  console.error("[Gateway] Failed to start:", error);
  process.exit(1);
}

process.on("unhandledRejection", (reason, promise) => {
  console.error(
    "[Gateway] Unhandled Rejection at:",
    promise,
    "reason:",
    reason,
  );
});

process.on("uncaughtException", (error) => {
  console.error("[Gateway] Uncaught Exception:", error);
});

export default app;
