#if !NET5_0_OR_GREATER

// ReSharper disable once CheckNamespace
namespace System.Runtime.CompilerServices;

using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

/// <summary>
/// Polyfill for <c>init</c>-only properties on older target frameworks.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
[ExcludeFromCodeCoverage]
internal static class IsExternalInit
{
}

#endif
