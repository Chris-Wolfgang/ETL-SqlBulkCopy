type: internal

The `nosemgrep` marker waiving the SQL-injection rule on `SqlConnectionCommandExecutor` now names the rule's full id and sits directly above the line it waives. Both are required for Semgrep to honour it, so the waiver had been inert and the finding kept reporting.
