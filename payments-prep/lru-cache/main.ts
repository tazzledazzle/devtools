class LRUCache<K, V> {
    private readonly store = new Map<K, V>();

    constructor(private readonly capacity: number) {
        if (capacity < 1) throw new RangeError(`capacity must be >= 1, got ${capacity}`);
    }

    get(key: K): V | undefined {
        if (!this.store.has(key)) return undefined;
        const value = this.store.get(key) as V;
        this.store.delete(key);
        this.store.set(key, value); // proceed to most recent tail
        return value;
    }

    set(key: K, value: V): void {
        if (this.store.has(key)) return undefined;
        this.store.set(key, value);
        if (this.store.size > this.capacity) {
            const eldest = this.store.keys().next().value as K;
            this.store.delete(eldest);  // evict last-recent
        }
    }
}