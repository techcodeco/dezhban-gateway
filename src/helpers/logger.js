import pino from "pino"; // یا هر logger دیگه‌ای

export const logger = pino({
  level: process.env.LOG_LEVEL || "info",
  formatters: {
    level: (label) => ({ level: label }),
  },
});
