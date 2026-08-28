export class TokenBucketLimiter {
  constructor({ capacity = 120, refillPerSecond = 2, maxKeys = 10_000, now = () => Date.now() } = {}) {
    if (!Number.isFinite(capacity) || capacity <= 0) throw new TypeError('capacity must be positive');
    if (!Number.isFinite(refillPerSecond) || refillPerSecond <= 0) throw new TypeError('refillPerSecond must be positive');
    if (!Number.isSafeInteger(maxKeys) || maxKeys < 1) throw new TypeError('maxKeys must be a positive safe integer');
    this.capacity = capacity;
    this.refillPerMs = refillPerSecond / 1000;
    this.maxKeys = maxKeys;
    this.now = now;
    this.buckets = new Map();
  }

  consume(key, cost = 1) {
    if (typeof key !== 'string' || !key) throw new TypeError('rate-limit key required');
    if (!Number.isFinite(cost) || cost <= 0 || cost > this.capacity) throw new TypeError('invalid rate-limit cost');
    const current = this.now();
    let bucket = this.buckets.get(key);
    if (!bucket) {
      if (this.buckets.size >= this.maxKeys) this.#evictOldest();
      bucket = { tokens: this.capacity, at: current, touched: current };
      this.buckets.set(key, bucket);
    }
    const elapsed = Math.max(0, current - bucket.at);
    bucket.tokens = Math.min(this.capacity, bucket.tokens + elapsed * this.refillPerMs);
    bucket.at = current;
    bucket.touched = current;
    if (bucket.tokens < cost) {
      const retryAfterMs = Math.ceil((cost - bucket.tokens) / this.refillPerMs);
      return { allowed: false, remaining: Math.floor(bucket.tokens), retryAfterMs };
    }
    bucket.tokens -= cost;
    return { allowed: true, remaining: Math.floor(bucket.tokens), retryAfterMs: 0 };
  }

  get trackedKeys() { return this.buckets.size; }

  #evictOldest() {
    let oldestKey;
    let oldest = Infinity;
    for (const [key, bucket] of this.buckets) {
      if (bucket.touched < oldest) { oldest = bucket.touched; oldestKey = key; }
    }
    if (oldestKey !== undefined) this.buckets.delete(oldestKey);
  }
}

export class ConcurrencyAdmissionController {
  constructor({ maxConcurrent = 256 } = {}) {
    if (!Number.isSafeInteger(maxConcurrent) || maxConcurrent < 1) throw new TypeError('maxConcurrent must be a positive safe integer');
    this.maxConcurrent = maxConcurrent;
    this.active = 0;
  }

  enter() {
    if (this.active >= this.maxConcurrent) return null;
    this.active += 1;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.active = Math.max(0, this.active - 1);
    };
  }
}

export function rateLimitError(result) {
  const error = new Error('rate limit exceeded');
  error.statusCode = 429;
  error.code = 'rate_limited';
  error.retryAfterMs = result.retryAfterMs;
  return error;
}
