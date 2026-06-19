import dotenv from "dotenv";
dotenv.config();
import { Elysia, t } from "elysia";
import { health } from "./controllers/health.controller.js";
import { webhook } from "./controllers/webhook.controller.js";

const WEBHOOK_SECRET_KEY = process.env.WEBHOOK_SECRET_KEY;

// دریافت شماره نمونه و پورت
const instanceId = parseInt(process.env.NODE_APP_INSTANCE || "0", 10);
const basePort = parseInt(process.env.PORT || "3000", 10);
const PORT = basePort + instanceId;

let WEBHOOK_SECRET_BUF = null;
if (WEBHOOK_SECRET_KEY) {
  WEBHOOK_SECRET_BUF = Buffer.from(WEBHOOK_SECRET_KEY);
} else {
  console.warn(
    "[Gateway] WEBHOOK_SECRET_KEY is not set. Webhook verification will be bypassed.",
  );
}

// --- Elysia App ---
const app = new Elysia()
  .decorate("webhookSecretBuf", WEBHOOK_SECRET_BUF)
  .get("/health", health)
  .post("/webhook/:secret", webhook, {
    params: t.Object({
      secret: t.String(),
    }),
  });

// روش صحیح اجرا برای Node.js
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
