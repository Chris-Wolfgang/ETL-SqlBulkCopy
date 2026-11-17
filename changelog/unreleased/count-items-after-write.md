type: fix

`CurrentItemCount` now advances only after a batch has been written to the server (it previously counted items as they were buffered). A progress report can no longer run ahead of the server, and after a failed batch the count excludes that batch, so resuming with `SkipItemCount` does not skip rows that were never written. `MaximumItemCount` still limits accepted items.
