type: fix

The source generator now emits `[ExcludeFromCodeCoverage]` on each generated `BulkCopyAccessors_*` type. Coverage tools were instrumenting the generated thunks like hand-written code, both here and in consumer assemblies that mark a type `[BulkCopyable]`.
