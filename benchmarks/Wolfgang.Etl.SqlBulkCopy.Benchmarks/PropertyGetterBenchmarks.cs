using System;
using System.Reflection;
using BenchmarkDotNet.Attributes;

namespace Wolfgang.Etl.SqlBulkCopy.Benchmarks;

/// <summary>
/// Per-row property access is the hottest path in a bulk load: the loader reads
/// every mapped property off every row. <see cref="ReflectionHelpers"/> compiles
/// a delegate once (per property) rather than paying <see cref="PropertyInfo"/>
/// dispatch on every row — see ADR-0004. This benchmark quantifies that choice:
/// the compiled getter vs the reflection call it replaces, for both a reference
/// and a value (boxed) property.
/// </summary>
[MemoryDiagnoser]
public class PropertyGetterBenchmarks
{
    private sealed record Row
    {
        public int Id { get; init; }

        public string Name { get; init; } = string.Empty;
    }

    private PropertyInfo _refProp = null!;
    private PropertyInfo _valueProp = null!;
    private Func<object, object?> _compiledRef = null!;
    private Func<object, object?> _compiledValue = null!;
    private Row _instance = null!;

    [GlobalSetup]
    public void Setup()
    {
        _refProp = typeof(Row).GetProperty(nameof(Row.Name))!;
        _valueProp = typeof(Row).GetProperty(nameof(Row.Id))!;
        _compiledRef = ReflectionHelpers.CompilePropertyGetter(_refProp);
        _compiledValue = ReflectionHelpers.CompilePropertyGetter(_valueProp);
        _instance = new Row { Id = 42, Name = "Acme" };
    }

    [Benchmark(Baseline = true)]
    public object? Reflection_Reference() => _refProp.GetValue(_instance);

    [Benchmark]
    public object? Compiled_Reference() => _compiledRef(_instance);

    [Benchmark]
    public object? Reflection_Value_Boxed() => _valueProp.GetValue(_instance);

    [Benchmark]
    public object? Compiled_Value_Boxed() => _compiledValue(_instance);
}
