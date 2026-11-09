type: fix

`TypeMapReader.GetFieldType` now carries the same `[return: DynamicallyAccessedMembers(PublicFields | PublicProperties)]` annotation as the `DbDataReader` member it overrides (net5.0 and later), clearing the IL2093 trim warning. No behaviour change.
