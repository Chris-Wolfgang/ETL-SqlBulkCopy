# Migrating from vX to vY

- **Applies to:** consumers upgrading `Wolfgang.Etl.SqlBulkCopy` from `X.*` to `Y.0.0`
- **Release:** [vY.0.0](https://github.com/Chris-Wolfgang/ETL-SqlBulkCopy/releases/tag/vY.0.0)
- **Related ADRs:** [ADR-NNNN](../adr/NNNN-....md)

> Copy this file to `vX-to-vY.md`, fill in every section, and delete these
> instruction blockquotes. Keep an entry for **every** breaking change — a
> consumer who hits an undocumented break loses trust in the whole guide.

## Summary

> One paragraph: what this major release changes at a high level and roughly how
> much work the upgrade is (drop-in, minor edits, or significant rework).

## Breaking-change inventory

> One row per break. "Kind" is Removed / Renamed / Signature changed /
> Behavior changed / Default changed.

| API | Kind | What changed |
|-----|------|--------------|
| `OldType.OldMethod(...)` | Renamed | now `NewType.NewMethod(...)` |
|  |  |  |

## Before / after

> A runnable before/after for each non-trivial break. Show the smallest real
> snippet that compiles on each side.

### `<change name>`

**Before (vX):**

```csharp
// old usage
```

**After (vY):**

```csharp
// new usage
```

## Deprecation timeline

> If APIs were deprecated in a prior minor before removal here, record when each
> was marked `[Obsolete]` and when it was removed, so consumers can see they had
> warning.

| API | Deprecated in | Removed in |
|-----|---------------|------------|
|  |  |  |

## Non-breaking additions worth adopting

> Optional: new APIs in vY that make the migrated code better. Not required to
> upgrade, but useful to mention so consumers get value from the bump.
