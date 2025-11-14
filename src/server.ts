import express from "express";
import { Notification, NotificationResult } from "./types";
import { MockProvider } from "./mockProvider";

const app = express();
app.use(express.json());

// Initialize provider
const mockProvider = new MockProvider();

// Stats tracking
let stats = {
  sent: 0,
  failed: 0,
  total: 0,
};

// Send notification endpoint
app.post("/notify", async (req, res) => {
  const notification: Notification = {
    id: `notif_${Date.now()}`,
    ...req.body,
  };

  stats.total++;

  try {
    let result: NotificationResult;

    // fire and forget - no retry logic!
    switch (notification.channel) {
      case "sms":
        result = await mockProvider.send(notification.channel, 0.3, 1000);
        break;
      case "push":
        result = await mockProvider.send(notification.channel, 0.2, 500);
        break;
      case "email":
        result = await mockProvider.send(notification.channel, 0.1, 200);
        break;
      default:
        throw new Error("Invalid channel");
    }

    stats.sent++;
    res.json({ success: true, result });
  } catch (error) {
    stats.failed++;
    console.error(`Failed to send ${notification.channel}:`, error.message);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// Stats endpoint
app.get("/stats", (req, res) => {
  res.json({
    ...stats,
    successRate:
      stats.total > 0
        ? ((stats.sent / stats.total) * 100).toFixed(2) + "%"
        : "0%",
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Phase 1: Notification service running on port ${PORT}`);
  console.log("⚠️  WARNING: No retry logic - messages will be lost!");
});
