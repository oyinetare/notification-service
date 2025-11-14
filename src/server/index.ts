import express from "express";
import { pool, initDB } from "../database";
import { Notification, NotificationResult } from "../types";
import { MockProvider } from "./mockProvider";
import { resolve } from "path";

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

// Queue notification instead of sending immediately
app.post("/notify", async (req, res) => {
  const { userId, channel, title, message, metadata } = req.body;

  const notificationId = `notif_${Date.now()}_${Math.random()
    .toString(36)
    .substring(2, 9)}`;

  try {
    // Store in database queue
    await pool.query(
      `INSERT INTO notifications 
        (notification_id, user_id, channel, title, message, metadata) 
        VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        notificationId,
        userId,
        channel,
        title,
        message,
        JSON.stringify(metadata || {}),
      ]
    );

    res.json({ success: true, notificationId, status: "queued" });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

async function processNotification(notification: any) {
  console.log(`Processing ${notification.notification_id}`);

  try {
    // Update status to processing
    await pool.query(
      "UPDATE notifications SET status = $1, updated_at = NOW() WHERE id = $2",
      ["processing", notification.id]
    );

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

    // Mark as sent
    await pool.query(
      "UPDATE notifications SET status = $1, updated_at = NOW() WHERE id = $2",
      ["sent", notification.id]
    );
  } catch (error) {
    // Mark as failed
    await pool.query(
      `UPDATE notifications 
       SET status = $1, error = $2, attempts = attempts + 1, updated_at = NOW() 
       WHERE id = $3`,
      ["failed", error.message, notification.id]
    );
  }
}

// Worker process - polls database
async function processQueue() {
  while (true) {
    try {
      // THIS IS THE PROBLEM: Scanning the entire table!
      const result = await pool.query(`
        SELECT * FROM notifications 
        WHERE status = 'pending' 
        ORDER BY created_at ASC 
        LIMIT 10
        FOR UPDATE SKIP LOCKED
        `);
      // FOR UPDATE SKIP LOCKED
      // SQL command used in databases like PostgreSQL and Oracle to
      // concurrently retrieve and lock rows without blocking other
      // transactions that attempt to access the same, already locked rows

      for (const row of result.rows) {
        await processNotification(row);
      }

      // Poll every second
      await new Promise((resolve) => setTimeout(resolve, 1000));
    } catch (error) {
      console.error("Queue processing error", error);
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
}

// Stats with query performance
app.get("/stats", async (req, res) => {
  const startTime = Date.now();

  const stats = await pool.query(`
    SELECT 
      status,
      COUNT(*) as count
    FROM notifications
    GROUP BY status
  `);

  const queryTime = Date.now() - startTime;

  res.json({
    stats: stats.rows,
    queryTimeMs: queryTime,
    warning: queryTime > 100 ? "SLOW QUERY DETECTED!" : null,
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  await initDB();
  console.log(`Phase 2: Notification service with DB queue ${PORT}`);
  console.log("⚠️  WARNING: Database polling will get slow!");

  // Start queue processor
  processQueue();
});
