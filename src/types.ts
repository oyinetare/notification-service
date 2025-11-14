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

export interface UserPreferences {
  userId: string;
  channels: {
    primary: Notification["channel"];
    fallback: Notification["channel"][];
  };
  limits: {
    // per diem
    sms: number;
    email: number;
  };
}

export interface FailoverNotification extends Notification {
  attemptedChannels?: string[];
  originalChannel?: string;
}
