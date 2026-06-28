using System.Diagnostics.CodeAnalysis;

// Exclude this entire test assembly from code coverage measurement.
// Coverage of test code is meaningless — what matters is the src code
// coverage these tests contribute to. Stage 2 (Windows) of pr.yaml runs
// integration tests; without this, the test assembly's own self-reported
// coverage (~17%) fails the 90% gate. Stage 1 sidesteps the issue by
// filtering integration projects out of the test run entirely.
[assembly: ExcludeFromCodeCoverage]
