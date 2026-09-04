// branded types prevent mixing up raw nums with dom vals
type Milliseconds = number & { readonly __brand: "Milliseconds"};
type Timestamp = number & { readonly __brand: "Timestamp" };

const ms = (n: number): Milliseconds => n as Milliseconds;
const now = (): Timestamp => Date.now() as Timestamp;

interface RateLimiterConfig {
    readonly maxRequests: number;
    readonly windowMs: Milliseconds;
}

class SlidingWindowRateLimiter {
    private readonly timestamps: Timestamp[] = [];

    constructor(private readonly config: RateLimiterConfig) {}
    
    allow(at: Timestamp = now()): boolean {
        const cutoff = (at - this.config.windowMs) as Timestamp;

        // evict the timestamps that have fallen outside the window
        while (this.timestamps.length > 0 && this.timestamps[0]! <= cutoff) {
            this.timestamps.shift();
        }

        if (this.timestamps.length < this.config.maxRequests) {
            this.timestamps.push(at);
            return true;
        }
        return false;
    }
}
