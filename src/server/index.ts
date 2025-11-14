import express from "express";
import Redis from "ioredis";
import Bull from "bull";
import { pool, initDB } from "../database";
import { Notification, NotificationResult } from "../types";
import { MockProvider } from "./mockProvider";
import { serverAdapter, notificationQueue } from "./queue";
import { RateLimiter } from "./rateLimiter";

const app = express();
app.use(express.json());

// Initialize provider
const mockProvider = new MockProvider();

const rateLimiter = new RateLimiter();

// Channel-specific rate limits
const RATE_LIMITS = {
  sms: {
    perUser: { limit: 1, window: 60 },
    global: { limit: 100, window: 60 },
  },
  email: {
    perUser: { limit: 10, window: 3600 },
    global: { limit: 1000, window: 60 },
  },
  push: {
    perUser: { limit: 20, window: 60 },
    global: { limit: 5000, window: 60 },
  },
};

const redisConfig = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: Number(process.env.REDIS_PORT) || 6379,
  password: process.env.REDIS_PASSWORD || undefined,
};

// Multiple queues with different priorities
const queues = {
  critical: new Bull("critical-notifications", { redis: redisConfig }),
  high: new Bull("high-notifications", { redis: redisConfig }),
  normal: new Bull("normal-notifications", { redis: redisConfig }),
  low: new Bull("low-notifications", { redis: redisConfig }),
};

// Dedicated worker pools per channel
const channelQueues = {
  sms: new Bull("sms-notifications", { redis: redisConfig }),
  email: new Bull("email-notifications", { redis: redisConfig }),
  push: new Bull("push-notifications", { redis: redisConfig }),
};

// Bull dashboard
app.use("/admin/queues", serverAdapter.getRouter());

// Queue notification instead of sending immediately
app.post("/notify", async (req, res) => {
  const { priority = "normal", channel } = req.body;

  type Priority = "critical" | "high" | "normal" | "low";
  const validPriorities: Priority[] = ["critical", "high", "normal", "low"];
  const selectedPriority: Priority = validPriorities.includes(priority)
    ? priority
    : "normal";

  try {
    const notification: Notification = {
      id: `notif_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      ...req.body,
    };

    // Add to priority queue
    const priorityQueue = queues[selectedPriority] || queues.normal;
    const job = await priorityQueue.add("process", req.body);

    // Add to Redis queue (instant!)
    // const job = await notificationQueue.add(notification, {
    //   attempts: 3,
    //   backoff: {
    //     type: "fixed",
    //     delay: 5000,
    //   },
    // });

    res.json({
      success: true,
      notificationId: notification.id,
      jobId: job.id,
      status: "queued",
      queue: priority,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
});

// Queue metrics endpoint
app.get("/queues/metrics", async (req, res) => {
  const metrics: {
    [key: string]: {
      waiting: number;
      active: number;
      completed: number;
      failed: number;
      delayed: number;
    };
  } = {};

  for (const [name, queue] of Object.entries({ ...queues, ...channelQueues })) {
    metrics[name] = {
      waiting: await queue.getWaitingCount(),
      active: await queue.getActiveCount(),
      completed: await queue.getCompletedCount(),
      failed: await queue.getFailedCount(),
      delayed: await queue.getDelayedCount(),
    };
  }

  res.json(metrics);
});

// Priority queue processor routes to channel queues
Object.entries(queues).forEach(([priority, queue]) => {
  queue.process("process", async (job) => {
    const notification = job.data;

    // Route to channel-specific queue
    const channel = notification.channel as keyof typeof channelQueues;
    if (!channelQueues[channel]) {
      throw new Error(`Invalid channel: ${notification.channel}`);
    }
    await channelQueues[channel].add(notification, {
      priority: priority === "critical" ? 1 : 0,
      attempts: priority === "critical" ? 5 : 3,
    });
  });
});

// Process each channel with dedicated workers
// channelQueues.sms.process(2, async (job) => {
//   // SMS is expensive, only 2 workers
//   return processSMS(job.data);
// });

// channelQueues.email.process(10, async (job) => {
//   // Email is cheap, 10 workers
//   return processEmail(job.data);
// });

// Process with rate limiting
// channelQueues.sms.process(2, async (job) => {
//   const notification = job.data;

//   // Check user rate limit
//   const userLimit = await rateLimiter.checkLimit(
//     `user:${notification.userId}:sms`,
//     RATE_LIMITS.sms.perUser.limit,
//     RATE_LIMITS.sms.perUser.window
//   );

//   if (!userLimit.allowed) {
//     // Delay the job
//     await job.moveToDelayed(userLimit.resetAt.getTime());
//     return { status: "rate_limited", resetAt: userLimit.resetAt };
//   }

//   // Check global rate limit
//   const globalLimit = await rateLimiter.checkLimit(
//     "global:sms",
//     RATE_LIMITS.sms.global.limit,
//     RATE_LIMITS.sms.global.window
//   );

//   if (!globalLimit.allowed) {
//     await job.moveToDelayed(Date.now() + 5000);
//     return { status: "global_rate_limited" };
//   }

//   // Priority notifications bypass rate limits
//   if (job.opts.priority > 0) {
//     console.log("High priority - bypassing rate limit");
//   }

//   return processSMS(notification);
// });

// Rate limit status endpoint
app.get("/rate-limits/:userId", async (req, res) => {
  const { userId } = req.params;
  const limits: { [channel: string]: any } = {};

  type Channel = keyof typeof RATE_LIMITS;
  const channels: Channel[] = ["sms", "email", "push"];
  for (const channel of channels) {
    limits[channel] = await rateLimiter.checkLimit(
      `user:${userId}:${channel}`,
      RATE_LIMITS[channel].perUser.limit,
      RATE_LIMITS[channel].perUser.window
    );
  }

  res.json(limits);
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
