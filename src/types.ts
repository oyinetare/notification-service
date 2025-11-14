export interface Notification {
  id: string;
  userId: string;
  channel: "sms" | "email" | "push";
  title?: string;
  message: string;
  metadata?: Record<string, any>;
}

export interface NotificationResult {
  id: string;
  status: "sent" | "failed";
  error?: string;
  timestamp: Date;
}
