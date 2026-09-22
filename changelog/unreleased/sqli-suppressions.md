type: internal

The two Semgrep SQL-injection findings are suppressed at source with a justification: `SqlConnectionCommandExecutor` runs the caller's own statement for pre/post-load custom actions (the same contract as `ExecuteSqlRaw`), and the sample harness passes literals.
