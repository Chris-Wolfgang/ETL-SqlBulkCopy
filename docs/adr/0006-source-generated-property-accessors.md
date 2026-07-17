# 0006. Source-generate property accessors for AOT-safe, zero-cost mapping

- **Status:** Proposed — targeted for 0.5.0+; supersedes [0004](0004-compiled-property-getters-and-native-aot.md) on acceptance
- **Date:** 2026-07-17

## Context

[ADR 0004](0004-compiled-property-getters-and-native-aot.md) chose
runtime-compiled expression-tree delegates for the per-row property and
enum-conversion hot path (the ~3–10x-per-property win that is the whole point of
a bulk-copy library) and accepted, as a consequence, that the library is "not
Native-AOT-compatible." Two of that ADR's claims turn out to be inaccurate, and
correcting them reframes the decision:

1. **`Expression.Compile()` does not throw under Native AOT.** The overload is
   annotated `[RequiresDynamicCode]`, so publishing a consumer with
   `<PublishAot>true</PublishAot>` surfaces `IL3050` warnings — but at runtime
   `Expression.Compile()` detects `RuntimeFeature.IsDynamicCodeSupported == false`
   and transparently falls back to the **expression interpreter**. The loader
   runs under AOT; it does not "throw at first use." 0004's stated failure mode
   is wrong.
2. **What AOT actually costs is the speed, not the correctness.** Under the
   interpreter fallback the getters run interpreted rather than JIT-compiled, so
   the entire performance rationale for compiling them evaporates and the hot
   path degrades toward reflection speed.

The forces, restated: we want to **keep the compiled-getter throughput** (the
priority) **and** run warning-free and fast under Native AOT. Runtime IL
emission cannot satisfy both. The remaining unknown for a full AOT publish is
`Microsoft.Data.SqlClient`, a reflection-heavy dependency that is not officially
AOT-supported — but that is outside this library's control and orthogonal to the
mapping layer.

A source generator moves the emission from runtime to **compile time**: it emits
the accessors as ordinary C# (`static object GetName(Person o) => o.Name;`),
which the normal compiler and JIT — or ILC under AOT — optimize to the same code
today's compiled delegates reach, with no `Expression`, no `Compile()`, no
`[RequiresDynamicCode]`, and no reflection. The emitted getter and the
expression-compiled getter produce byte-identical IL, so there is no semantic
fork between the two mechanisms.

## Decision

Introduce a **Roslyn incremental source generator** that emits strongly-typed
property accessors and column descriptors at compile time for opt-in types, and
make **generated metadata the preferred provider** with the existing
reflection + `Expression.Compile` path retained as the **runtime fallback**.

Specifically:

- **One package, one public API, one `TypeMap`.** The generator ships as an
  analyzer asset inside `Wolfgang.Etl.SqlBulkCopy` — not a separate package. The
  public surface (`SqlBulkCopyLoader<T>` etc.) is unchanged.
- **Opt-in discovery.** A marker attribute (e.g. `[BulkCopyable]`) on the record
  is what the generator keys off, rather than analyzing every
  `SqlBulkCopyLoader<T>` construction site.
- **Two metadata sources, not two designs.** The generator emits only the thin
  front-end — column descriptors plus the getter delegates — and feeds them into
  the *existing* `TypeMap`/reader machinery. Qualified-name formatting,
  bracket-escaping, duplicate-column detection, caching, and validation stay
  single-sourced in the runtime code. At runtime, `TypeMap` prefers a generated
  map when one is present and falls back to reflection when it is not.
- **Single-sourced rule contract.** The one genuinely duplicated piece — reading
  the mapping attributes off a type — cannot share code, because the generator
  works over Roslyn's `INamedTypeSymbol` model and the runtime over
  `System.Reflection.Type`. The two front-ends are kept in sync by a written
  mapping-rule spec plus a **conformance test** asserting both produce identical
  `TypeMap`s for a corpus of sample records.

## Consequences

- **The x60/compiled-getter throughput is preserved on both JIT and AOT** for
  any type the generator can see. On AOT the hot path is native and
  `IL3050`-free; startup cost drops too, since nothing is compiled at first use.
- **The runtime path cannot be deleted without a breaking capability cut.** A
  source generator can only emit accessors for types that exist in source at
  compile time. Types constructed dynamically —
  `Activator.CreateInstance(typeof(SqlBulkCopyLoader<>).MakeGenericType(runtimeType))`,
  or an open generic parameter that only closes in downstream user code — are
  invisible to it and must use the reflection fallback. Dropping that path would
  narrow the library from a general ETL loader to a "statically-known types
  only" loader. This distinction is the *only* irreducible one between the two
  mechanisms.
- **That fallback gap is already inherent to AOT.** The same dynamic scenarios
  (`MakeGenericType` over a non-statically-rooted type) are exactly what Native
  AOT cannot preserve regardless of this library, so "generated-only" is the
  effective contract under AOT anyway; the reflection fallback earns its keep on
  **JIT**.
- **The accepted maintenance tax is one duplicated front-end** (attribute
  reading against two type models), bounded and guarded by the conformance
  tests. Everything downstream of "read the attributes" remains single-sourced.
- **`Microsoft.Data.SqlClient` AOT-readiness remains a separate, external
  question.** A full end-to-end AOT publish that opens a real connection may hit
  issues in SqlClient independent of this mapping work; this ADR does not claim
  to resolve that.
- On acceptance and implementation, this ADR **supersedes 0004**: compiled
  expression-tree getters remain the JIT-platform default provider, but the
  "not AOT-compatible / throws at first use" conclusion is retired.

Tracked by issue [#95](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/issues/95).
