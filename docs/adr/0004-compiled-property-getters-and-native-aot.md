# 0004. Compile property getters with expression trees

- **Status:** Accepted
- **Date:** 2026-07-10

## Context

Mapping a `TRecord` to SQL columns means reading each mapped property off every
row. The obvious implementation, `PropertyInfo.GetValue(object)`, pays the full
reflection dispatch cost on every property of every row — the hottest path in a
bulk load of millions of rows. `ReflectionHelpers.CompilePropertyGetter`
instead builds an `Expression.Lambda<Func<object, object?>>` once per property
and `.Compile()`s it to a delegate, paying the dispatch cost a single time; the
same technique compiles enum-to-underlying-type converters. Measured speedup is
roughly 3–10x per property depending on shape.

`.Compile()` emits IL at runtime. Native AOT (`<PublishAot>true</PublishAot>`)
and aggressive trimming forbid runtime IL generation.

## Decision

We use runtime-compiled expression-tree delegates for property and enum access,
accepting that the library is **not Native-AOT-compatible** and is not verified
under `<PublishTrimmed>`.

## Consequences

- The per-row hot path avoids reflection dispatch, which is the right call for a
  bulk-copy library whose entire purpose is throughput.
- Consumers targeting Native AOT cannot use this library as-is; the loader would
  throw at first use when the runtime cannot emit the getter. This is a
  documented limitation, not a bug.
- Making the library AOT-safe later would mean replacing the compiled getters
  with either a source generator (compile-time getters, no runtime emit) or
  plain `PropertyInfo.GetValue` (AOT-safe but slow). That is a deliberate future
  decision, tracked separately, not something to bolt on reactively. The
  `[NoAlloc]`/AOT-smoke test discussions in the maintenance backlog are scoped
  against this ADR.
