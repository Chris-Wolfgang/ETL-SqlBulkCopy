type: fix

The opt-in data-validation path (`EnableDataValidation`) now states its trimming requirement: it uses the reflection-based DataAnnotations `Validator`, so a trimmed or Native-AOT app that enables it must preserve the record type's public properties and validation attributes. The IL2026 trim warnings on the two validation helpers are suppressed with that justification. No behaviour change.
