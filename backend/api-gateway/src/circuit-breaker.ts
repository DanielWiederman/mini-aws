export class CircuitOpenError extends Error {
  constructor() {
    super('Circuit breaker OPEN — Kafka unavailable');
    this.name = 'CircuitOpenError';
  }
}

type CircuitState = 'CLOSED' | 'OPEN' | 'HALF_OPEN';

export class CircuitBreaker {
  private state: CircuitState = 'CLOSED';
  private failures = 0;
  private lastFailureTime = 0;
  private readonly threshold = 5;
  private readonly resetTimeout = 30000; // 30 seconds

  async call<T>(fn: () => Promise<T>): Promise<T> {
    if (this.state === 'OPEN') {
      if (Date.now() - this.lastFailureTime > this.resetTimeout) {
        this.state = 'HALF_OPEN';
      } else {
        throw new CircuitOpenError();
      }
    }
    try {
      const result = await fn();
      this.failures = 0;
      this.state = 'CLOSED';
      return result;
    } catch (err) {
      this.failures++;
      this.lastFailureTime = Date.now();
      if (this.failures >= this.threshold) {
        this.state = 'OPEN';
        console.error(`🌐 [Circuit Breaker] OPENED after ${this.failures} consecutive failures`);
      }
      throw err;
    }
  }

  getState(): CircuitState {
    return this.state;
  }
}
