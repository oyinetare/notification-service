import Redis from "ioredis";

export class RateLimiter {
  private redis = new Redis();

  async checkLimit(
    key: string,
    limit: number,
    windowSeconds: number
  ): Promise<{ allowed: boolean; remaining: number; resetAt: Date }> {
    const now = Date.now();
    const window = Math.floor(now / (windowSeconds * 1000));
    const redisKey = `rate:${key}:${window}`;

    const current = await this.redis.incr(redisKey);

    if (current === 1) {
      await this.redis.expire(redisKey, windowSeconds);
    }

    return {
      allowed: current <= limit,
      remaining: Math.max(0, limit - current),
      resetAt: new Date((window + 1) * windowSeconds * 1000),
    };
  }
}
