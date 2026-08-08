// Polyfill for the compiler-required IsExternalInit type so that record types
// and init-only setters compile on netstandard2.0 (the Roslyn analyzer target
// framework, which predates this type in the BCL).
namespace System.Runtime.CompilerServices
{
    using System.ComponentModel;

    [EditorBrowsable(EditorBrowsableState.Never)]
    internal static class IsExternalInit
    {
    }
}
