import express from "express";
import { pool, initDB } from "../database";
import { Notification, NotificationResult } from "../types";
import { MockProvider } from "./mockProvider";
import { serverAdapter, notificationQueue } from "./queue";

const app = express();
app.use(express.json());

// Initialize provider
const mockProvider = new MockProvider();

// Bull dashboard
app.use("/admin/queues", serverAdapter.getRouter());

// Queue notification instead of sending immediately
app.post("/notify", async (req, res) => {
  try {
    const notification: Notification = {
      id: `notif_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      ...req.body,
    };

    // Add to Redis queue (instant!)
    const job = await notificationQueue.add(notification, {
      attempts: 3,
      backoff: {
        type: "fixed",
        delay: 5000,
      },
    });

    res.json({
      success: true,
      notificationId: notification.id,
      jobId: job.id,
      status: "queued",
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// Process queue with idempotency
notificationQueue.process(10, async (job) => {
  const notification = job.data;

  // Check if already sent
  const existing = await pool.query(
    "SELECT * FROM notification_sends WHERE notification_id = $1 AND channel = $2",
    [notification.id, notification.channel]
  );

  if (existing.rows.length > 0) {
    console.log(`Already sent ${notification.id} - skipping`);
    return {
      status: "already_sent",
      providerId: existing.rows[0].provider_id,
    };
  }

  console.log(
    `Processing ${notification.id} (attempt ${job.attemptsMade + 1})`
  );

  // Store in DB for history
  await pool.query(
    `INSERT INTO notifications 
     (notification_id, user_id, channel, title, message, metadata, status) 
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (notification_id) 
     DO UPDATE SET attempts = notifications.attempts + 1`,
    [
      notification.id,
      notification.userId,
      notification.channel,
      notification.title,
      notification.message,
      JSON.stringify(notification.metadata || {}),
      "processing",
    ]
  );

  try {
    // Send notification
    let result;
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

    // Update status
    await pool.query(
      "UPDATE notifications SET status = $1 WHERE notification_id = $2",
      ["sent", notification.id]
    );

    // Record successful send
    await pool.query(
      "INSERT INTO notification_sends (notification_id, channel, provider_id) VALUES ($1, $2, $3)",
      [notification.id, notification.channel, result.id]
    );

    return result;
  } catch (error: any) {
    // Update failure status
    await pool.query(
      "UPDATE notifications SET status = $1, error = $2 WHERE notification_id = $3",
      ["failed", error.message, notification.id]
    );

    throw error; // Bull will retry - only throw if not already sent
  }
});

// Queue stats
app.get("/stats", async (req, res) => {
  const [waiting, active, completed, failed] = await Promise.all([
    notificationQueue.getWaitingCount(),
    notificationQueue.getActiveCount(),
    notificationQueue.getCompletedCount(),
    notificationQueue.getFailedCount(),
  ]);

  const dbStats = await pool.query(`
    SELECT status, COUNT(*) as count
    FROM notifications
    GROUP BY status
  `);

  const checkDuplicates = await pool.query(`
    SELECT notification_id, COUNT(*) as count
    FROM notifications
    GROUP BY notification_id
    HAVING COUNT(*) > 1
  `);

  res.json({
    queue: { waiting, active, completed, failed },
    database: dbStats.rows,
    duplicateSends: await checkDuplicates.rows, // Problem indicator!
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  await initDB();
  console.log("Phase 3: Redis queue with Bull");
  console.log("⚠️  WARNING: Retries will cause duplicate sends!");
  console.log("📊 Queue dashboard: http://localhost:3000/admin/queues");
});
