import Bull from "bull";
import { Notification } from "../types";

export const notificationQueue = new Bull<Notification>("notifications", {
  redis: {
    host: "localhost",
    port: 6379,
  },
});

// Queue UI for monitoring
import { createBullBoard } from "@bull-board/api";
import { BullAdapter } from "@bull-board/api/bullAdapter";
import { ExpressAdapter } from "@bull-board/express";

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath("/admin/queues");

createBullBoard({
  queues: [new BullAdapter(notificationQueue)],
  serverAdapter,
});

export { serverAdapter };
