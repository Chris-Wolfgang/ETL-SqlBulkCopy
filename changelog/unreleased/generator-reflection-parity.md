type: fix

The `[BulkCopyable]` generator now maps only public properties (matching the reflection path), falls back to reflection for a property with both `[NotMapped]` and `[Column]` so the error is reported, and compiles for keyword-named properties.
