type: internal

The Semgrep SQL-injection waiver for `SqlConnectionCommandExecutor` (which runs the caller's own pre/post-load statement by design) now lives in the Semgrep workflow configuration instead of an inline `nosemgrep` marker.
