# Architecture Decision Records

This directory records the non-obvious design decisions behind
`Wolfgang.Etl.SqlBulkCopy` — the context, the decision, and the consequences —
so future maintainers (and the author six months later) don't re-derive the
same trade-offs from scratch.

The format is a lightweight variant of
[MADR](https://adr.github.io/madr/): each record is a short, numbered,
immutable markdown file. Decisions are never edited to say something different
after the fact; instead a later ADR **supersedes** an earlier one and the older
record is marked accordingly.

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-isolate-sqlbulkcopy-sdk-behind-wrapper-interfaces.md) | Isolate the SqlBulkCopy SDK behind wrapper interfaces | Accepted |
| [0003](0003-enforce-async-only-io-via-banned-symbols.md) | Enforce async-only I/O via BannedSymbols.txt | Accepted |
| [0004](0004-compiled-property-getters-and-native-aot.md) | Compile property getters with expression trees | Superseded by [0006](0006-source-generated-property-accessors.md) (0.5.0) |
| [0005](0005-decouple-assemblyversion-from-package-version.md) | Decouple AssemblyVersion from the package version | Accepted |
| [0006](0006-source-generated-property-accessors.md) | Source-generate property accessors for AOT-safe, zero-cost mapping | Accepted (0.5.0) |

## Adding a new ADR

1. Copy [`TEMPLATE.md`](TEMPLATE.md) to `NNNN-short-kebab-title.md`,
   taking the next free number.
2. Fill in Context, Decision, Consequences.
3. Add a row to the index table above.
4. If the ADR reverses an earlier one, set the older ADR's status to
   `Superseded by [NNNN](...)` and link back.
