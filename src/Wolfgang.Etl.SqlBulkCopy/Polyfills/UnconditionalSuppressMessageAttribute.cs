// Polyfill so the runtime library can suppress trim warnings on its reflection
// fallback on every target framework. This attribute ships in the BCL from
// net5.0; the #if guard emits an internal copy only on the older targets
// (net462 / net481 / netstandard2.0) that lack it.
#if !NET5_0_OR_GREATER
// ReSharper disable once CheckNamespace -- polyfill mirrors BCL type location.
namespace System.Diagnostics.CodeAnalysis
{
    [AttributeUsage(AttributeTargets.All, AllowMultiple = true, Inherited = false)]
    internal sealed class UnconditionalSuppressMessageAttribute : Attribute
    {
        public UnconditionalSuppressMessageAttribute(string category, string checkId)
        {
            Category = category;
            CheckId = checkId;
        }



        // Getters below are read by tooling (trim/AOT analyzers) cross-assembly,
        // not by source in this DLL.

        // ReSharper disable once UnusedAutoPropertyAccessor.Global
        public string Category { get; }



        // ReSharper disable once UnusedAutoPropertyAccessor.Global
        public string CheckId { get; }



        public string? Scope { get; set; }



        public string? Target { get; set; }



        public string? MessageId { get; set; }



        // ReSharper disable once UnusedAutoPropertyAccessor.Global
        public string? Justification { get; set; }
    }
}
#endif
