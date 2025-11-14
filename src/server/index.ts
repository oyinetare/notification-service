import express from "express";
import { pool, initDB } from "../database";
import { FailoverNotification, UserPreferences } from "../types";
import { MockProvider } from "./providers/mockProvider";
import { serverAdapter, notificationQueue } from "./queue";
import { ResilientProvider } from "./providers/ResilientProvider";

const app = express();
app.use(express.json());

// Initialize provider
const mockProvider = new MockProvider();
const resilientProvider = new ResilientProvider();

// User preferences (in-memory for demo)
const userPreferences: Map<string, UserPreferences> = new Map([
  [
    "user1",
    {
      userId: "user1",
      channels: {
        primary: "push",
        fallback: ["sms", "email"],
      },
      limits: { sms: 10, email: 50 },
    },
  ],
]);

// Bull dashboard
app.use("/admin/queues", serverAdapter.getRouter());

// Queue notification instead of sending immediately
app.post("/notify", async (req, res) => {
  try {
    const { userId, message, priority } = req.body;

    const prefs = userPreferences.get(userId) || {
      channels: { primary: "email", fallback: ["sms"] },
    };

    // const notification: Notification = {
    //   id: `notif_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
    //   ...req.body,
    // };

    const notification: FailoverNotification = {
      id: `notif_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      userId,
      channel: prefs.channels.primary,
      originalChannel: prefs.channels.primary,
      message,
      attemptedChannels: [],
      metadata: req.body.metadata,
    };

    // Add to Redis queue (instant!)
    const job = await notificationQueue.add(notification, {
      attempts: 3,
      // Configure exponential backoff
      backoff: {
        type: "exponential",
        delay: 2000, // 2s, 4s, 8s, 16s, 32s
      },
      removeOnComplete: true,
      removeOnFail: false,
      priority: priority === "high" ? 1 : 0,
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

// Monitor worker health
let workerStats = {
  processing: new Set(),
  blocked: 0,
};

// Process queue with idempotency + failover
notificationQueue.process("notification", async (job) => {
  const notification: FailoverNotification = job.data;

  workerStats.processing.add(job.id);

  // Check if already sent successfully on any channel
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

  // Try current channel
  notification.attemptedChannels!.push(notification.channel);

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
    // If all workers stuck on same channel
    if (
      workerStats.processing.size === 5 &&
      Array.from(workerStats.processing).every(
        (id) =>
          // Check if all processing same channel
          true // simplified
      )
    ) {
      console.log("⚠️  HEAD-OF-LINE BLOCKING DETECTED!");
      workerStats.blocked++;
    }

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

    // await sendViaChannel(notification);
    // return result;
    return { status: "sent", channel: notification.channel };
  } catch (error: any) {
    console.log(`Failed ${notification.channel}: ${error.message}`);

    // Get next fallback channel
    const prefs = userPreferences.get(notification.userId);
    const remaining = prefs!.channels.fallback.filter((ch) =>
      notification.attemptedChannels!.includes(ch)
    );

    if (remaining.length > 0) {
      // Failover to next channel
      notification.channel = remaining[0];

      // Re-queue with updated channel
      await notificationQueue.add("notification", notification, {
        delay: 5000, // Wait 5s before retry
        priority: job.opts.priority,
      });

      return { status: "failover", nextChannel: notification.channel };
    }

    throw new Error("All channels failed");
  } finally {
    workerStats.processing.delete(job.id);
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

  const health = { _: resilientProvider.getHealth() };

  const status = Object.values(health).every(
    (h) => h.state === "closed" || h.state === undefined
  )
    ? 200
    : 503;

  res.json({
    queue: { waiting, active, completed, failed },
    database: dbStats.rows,
    providerHealthStaus: status,
    duplicateSends: await checkDuplicates.rows, // Problem indicator!
  });
});

// async function selectAvailableChannel(preferredChannels: string[]) {
//   for (const channel of preferredChannels) {
//     const provider = getProvider(channel);
//     if (provider.getHealth().state !== "open") {
//       return channel;
//     }
//   }
//   return null; // All circuits open!
// }

const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
  await initDB();
  console.log("Phase 3: Redis queue with Bull");
  console.log("⚠️  WARNING: Retries will cause duplicate sends!");
  console.log("📊 Queue dashboard: http://localhost:3000/admin/queues");
});
