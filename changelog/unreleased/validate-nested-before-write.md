type: fix

With `EnableDataValidation` and the default `ValidationFailureBehavior.Throw`, a nested child (at any depth) that fails validation now throws before any row of its parent's batch is written, instead of after the parent rows were sent. `Skip` is unchanged.
