using System;
using System.Collections.Concurrent;
using System.ComponentModel;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Infrastructure for source-generated type descriptors. The
/// <c>Wolfgang.Etl.SqlBulkCopy</c> source generator emits a call to
/// <see cref="Register"/> from a module initializer for each <c>[BulkCopyable]</c>
/// type; <c>TypeMap.Create</c> prefers a registered descriptor and falls back to
/// reflection only for unregistered types. Not intended for direct use.
/// </summary>
/// <remarks>
/// Public because generated code runs in the consumer's assembly, which has no
/// <c>InternalsVisibleTo</c> access here; hidden from IntelliSense to signal it
/// is not part of the application-facing surface.
/// </remarks>
[EditorBrowsable(EditorBrowsableState.Never)]
public static class GeneratedTypeMapRegistry
{
    private static readonly ConcurrentDictionary<Type, GeneratedTypeDescriptor> Descriptors = new();



    /// <summary>
    /// Registers the generated descriptor for <paramref name="type"/>. Intended
    /// to be called only by generated code.
    /// </summary>
    /// <exception cref="ArgumentNullException">
    /// Thrown when any argument is <see langword="null"/>.
    /// </exception>
    public static void Register(Type type, GeneratedTypeDescriptor descriptor)
    {
        if (type is null)
        {
            throw new ArgumentNullException(nameof(type));
        }

        if (descriptor is null)
        {
            throw new ArgumentNullException(nameof(descriptor));
        }

        Descriptors[type] = descriptor;
    }



    /// <summary>
    /// Attempts to retrieve the generated descriptor registered for <paramref name="type"/>.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when a descriptor has been registered; otherwise
    /// <see langword="false"/>.
    /// </returns>
    internal static bool TryGet(Type type, out GeneratedTypeDescriptor descriptor)
    {
        return Descriptors.TryGetValue(type, out descriptor!);
    }
}
