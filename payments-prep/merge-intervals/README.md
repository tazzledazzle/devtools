# Pattern 3 — Transaction Validation / Data Integrity

**Domain:** CSV parsing, field validation, behavioral baseline matching

Confirmed from a LeetCode discuss post describing a four-part Stripe phone screen (see leetcode.com/discuss/post/7384225).

## Parts

**Part 1 — Basic field validation** Read transactions from a CSV with six fields. Flag any row as SUSPICIOUS if any field is empty or malformed.

**Part 2 — Business rule validation** Apply additional rules: transaction amount must fall within a defined normal range, payment method must not appear in a blocked methods list.

**Part 3 — Behavioral baseline matching** Validate whether the transaction matches the user's historical behavior. At least 50% of behavioral attributes — spending countries, typical time ranges, average transaction amount intervals — must match the user's baseline. Requires feature extraction, normalization, and ratio computation.

**Part 4 — Smart error reporting** Replace the generic SUSPICIOUS label with specific error codes. Output up to two error codes in priority order. If no issues, output OK. Maintain column alignment in the output report.
