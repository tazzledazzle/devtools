interface Interval {
    readonly start: number;
    readonly end: number;
}

function mergeIntervals(intervals: readonly Interval[]): Interval[] {
    if (intervals.length === 0) return [];

    const sorted = [...intervals].sort((a,b) => a.start - b.start);
    const merged: Interval[] = [sorted[0]!];

    for (const current of sorted.slice(1)) {
        const last = merged.at(-1)!;
        if (current.start <= last.end) {
            merged[merged.length - 1] = { start: last.start, end: Math.max(last.end, current.end) };
        } else {
            merged.push(current);
        }
    }
    return merged;
}