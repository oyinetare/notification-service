import { CircuitBreaker } from "../circuit-breaker";
import { MockProvider } from "./mockProvider";

export class ResilientProvider {
  private circuitBreaker = new CircuitBreaker();
  private provider = new MockProvider();

  async send() {
    return this.circuitBreaker.execute(async () => {
      return this.provider.send("sms", 0.3, 1000);
    });
  }

  getHealth() {
    return this.circuitBreaker.getState();
  }
}
