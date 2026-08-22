// Polyfill so the runtime library can mark its reflection-based mapping path
// [RequiresUnreferencedCode] on every target framework. This attribute ships in
// the BCL from net5.0; the #if guard emits an internal copy only on the older
// targets (net462 / net481 / netstandard2.0) that lack it.
#if !NET5_0_OR_GREATER
// ReSharper disable once CheckNamespace -- polyfill mirrors BCL type location.
namespace System.Diagnostics.CodeAnalysis
{
    [AttributeUsage
    (
        AttributeTargets.Constructor | AttributeTargets.Method | AttributeTargets.Class,
        Inherited = false
    )]
    internal sealed class RequiresUnreferencedCodeAttribute : Attribute
    {
        public RequiresUnreferencedCodeAttribute(string message)
        {
            Message = message;
        }



        // ReSharper disable once UnusedAutoPropertyAccessor.Global -- read by
        // the trim/AOT analyzers cross-assembly, not from source in this DLL.
        public string Message { get; }



        public string? Url { get; set; }
    }
}
#endif
