type: internal

`AssemblyVersion` and `FileVersion` are now derived from `<Version>` (`0.{Minor}.0.0` / `{Version}.0`) instead of hand-pinned, so they can no longer fall behind on a minor bump.
