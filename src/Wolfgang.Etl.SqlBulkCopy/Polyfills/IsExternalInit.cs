#if !NET5_0_OR_GREATER

using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

// ReSharper disable once CheckNamespace -- polyfill mirrors BCL type location.
namespace System.Runtime.CompilerServices;

/// <summary>
/// Polyfill for <c>init</c>-only properties on older target frameworks.
/// </summary>
/// <remarks>
/// This is a compiler-recognized marker type with no body — the C# compiler
/// looks for any type named <c>System.Runtime.CompilerServices.IsExternalInit</c>
/// to enable <c>init</c> accessors on netstandard2.0 / net462. There is no
/// executable code to cover, so <see cref="ExcludeFromCodeCoverageAttribute"/>
/// is the correct (and only) way to keep this out of the coverage denominator.
/// </remarks>
[EditorBrowsable(EditorBrowsableState.Never)]
[ExcludeFromCodeCoverage]
internal static class IsExternalInit
{
}

#endif
