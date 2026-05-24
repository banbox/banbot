# Repo Guidance

## QuestDB WAL Rules

- Treat QuestDB `WAL` tables as read-after-write asynchronous. A successful `INSERT` or `CREATE TABLE ... AS` does not mean a follow-up read will immediately see the new rows.
- Do not make fallback or destructive decisions from a single immediate read after writing QuestDB tables. If behavior depends on visibility, wait for the expected row, timestamp, range, or count to become visible first.
- Before `DROP` / `RENAME` table-swap flows, verify the replacement table matches the expected snapshot. Never delete the old table on an unchecked assumption.
- Recovery markers such as pending insert jobs must be preserved when visibility checks time out. Do not clear recovery state just because a first read returns empty.
- When a same-process write is followed by a same-process read on QuestDB metadata tables, prefer a targeted visibility wait or an in-process cache/lock over blind retry loops.

## Testing Expectations

- Any fix for QuestDB visibility or table-swap logic should include a targeted regression test for the timeout/polling path or the pre-drop verification path.
