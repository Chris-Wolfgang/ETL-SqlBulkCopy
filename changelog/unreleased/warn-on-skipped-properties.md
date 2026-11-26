type: feature

`SqlBulkCopyLoader` now logs one warning (event id 107) per record type naming the public properties it skips because their type is not a supported column type, on both the reflection and `[BulkCopyable]` paths; `GeneratedTypeDescriptor` gains `SkippedPropertyNames` and a constructor overload that carries them.
