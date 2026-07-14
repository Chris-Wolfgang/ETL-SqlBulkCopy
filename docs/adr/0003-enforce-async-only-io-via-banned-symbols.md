# 0003. Enforce async-only I/O via BannedSymbols.txt

- **Status:** Accepted
- **Date:** 2026-07-10

## Context

`SqlBulkCopyLoader<T>` consumes an `IAsyncEnumerable<T>` and writes it to SQL
Server. Every I/O operation on that path — opening the connection, writing a
batch, running a pre/post SQL command — has both a synchronous and an
asynchronous form on the underlying SDK types. A single stray synchronous call
(`SqlBulkCopy.WriteToServer` instead of `WriteToServerAsync`,
`Task.Result`, `Thread.Sleep`) blocks a pooled thread and can deadlock a
consumer running in an async context. These regressions are invisible in review
and only show up under load.

## Decision

We ship a `BannedSymbols.txt` consumed by
`Microsoft.CodeAnalysis.BannedApiAnalyzers` that fails the build on synchronous
blocking APIs: `Task.Wait`/`Task.Result`, `Task.WaitAll`/`WaitAny`,
`Thread.Sleep`, synchronous `File`/stream I/O, `Parallel.For`/`ForEach`, and
similar. Warnings are treated as errors, so a banned call cannot merge.

## Consequences

- The async-only contract is mechanically enforced, not merely documented; a
  contributor who reaches for a synchronous API is stopped at build time with a
  message pointing at the async alternative.
- The ban list is per-repo and must be kept in sync with the fleet's canonical
  list; `BannedSymbols.txt` is a protected file, so changes to it go through a
  guarded PR.
- A genuinely-needed synchronous call (there are none today) would require an
  explicit, reviewed suppression rather than slipping in silently — which is the
  point.
