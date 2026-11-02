type: fix
The package now depends on `System.Security.Cryptography.Pkcs` 9.0.21 instead of the vulnerable 8.0.1/9.0.18 that `Microsoft.Data.SqlClient` brings in (CVE-2026-84008, CVE-2026-70162).
