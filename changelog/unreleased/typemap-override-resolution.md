type: internal

`TypeMap` resolves table and schema overrides with `??`: `TypeMap.Create` already normalizes empty and whitespace overrides to null, so the second whitespace check in each resolver was unreachable. No behaviour change.
