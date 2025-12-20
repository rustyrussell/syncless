# syncless: Ordered, atomic storage without durability guarantees.

`syncless` provides crash-safe, append-style storage where:
- individual writes are atomic
- writes are observed in order
- previously visible data is never corrupted

Recent writes may be lost on crash or power failure.

## When to use this

Use `syncless` when:
- durability is not required
- corruption is unacceptable
- synchronous `fsync` is too expensive

Examples: browser history, bookmarks, caches, indexes.

## When not to use this

Do not use `syncless` when you need:
- durability guarantees
- multi-writer isolation
- cross-process coordination

Try sqlite3 for that.

## Documentation

Full API documentation: <https://docs.rs/syncless>
