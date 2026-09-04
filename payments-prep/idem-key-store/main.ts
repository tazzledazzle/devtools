type Milliseconds = number & { readonly __brand: "Milliseconds"};
const ms = (n: number): Milliseconds => n as Milliseconds;

export const Duration = {
    minutes: (n: number): Milliseconds => ms(n * 60_000),
    hours: (n: number): Milliseconds => ms(n * 3_600_000),
    seconds: (n: number): Milliseconds => ms(n * 1_000),
} as const;

interface CachedResult<T> {
    readonly response: T;
    readonly expiresAt: Date;
}

export class IdempotencyStore<T = string> {
    private readonly store = new Map<string, CachedResult<T>>();

    constructor(private readonly ttl: Milliseconds = Duration.minutes(10)) {}

    getOrCompute(key: string, compute: () => T): T {
        const now = new Date();
        const existing = this.store.get(key);

        if (existing !== undefined && now < existing.expiresAt) {
            return existing.response; // replay - do NOT recompute charde
        }

        const response = compute();
        this.store.set(key, {
            response,
            expiresAt: new Date(now.getTime() + this.ttl),
        });
        return response;
    }
}
