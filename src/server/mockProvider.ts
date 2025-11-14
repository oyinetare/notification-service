import { Notification, NotificationResult } from "../types";

export class MockProvider {
  async send(
    channel: Notification["channel"],
    failureRate: number,
    latencyMs: number
  ): Promise<NotificationResult> {
    // Simulate network latency
    await new Promise((resolve) => setTimeout(resolve, latencyMs));

    // Simulate random failures
    if (Math.random() < failureRate) {
      switch (channel) {
        case "sms":
          throw new Error("SMS Gateway timeout");
        case "push":
          throw new Error("Push service error: Invalid device token");
        case "email":
          throw new Error("Email server unavailable");
        default:
          throw new Error("Invalid channel");
      }
    }

    return {
      id: `${channel}_${Date.now()}`,
      status: "sent",
      timestamp: new Date(),
    };
  }
}
