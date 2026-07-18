using System;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Opts a type into compile-time source-generated property accessors. When a
/// type is marked with this attribute, the <c>Wolfgang.Etl.SqlBulkCopy</c>
/// source generator emits strongly-typed getters for its mappable properties
/// and registers them with <see cref="GeneratedAccessorRegistry"/> at module
/// load, so the bulk-copy hot path reads values without emitting IL at runtime.
/// </summary>
/// <remarks>
/// <para>
/// The attribute is purely opt-in and additive: an unmarked type continues to
/// work exactly as before, using a getter compiled at runtime with
/// <see cref="System.Linq.Expressions"/>. Marking a type does not change its
/// mapping — it only changes how the getters are produced (compile time vs
/// runtime), which is what makes the marked type's hot path Native-AOT clean.
/// See ADR 0006.
/// </para>
/// <para>
/// Generated registration relies on module initializers, which are available
/// on <c>net5.0</c> and later. On earlier target frameworks the generator emits
/// nothing and the type falls back to the runtime-compiled getter — correct on
/// those JIT-only targets, where runtime IL emission is not a concern.
/// </para>
/// </remarks>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
public sealed class BulkCopyableAttribute : Attribute
{
}
