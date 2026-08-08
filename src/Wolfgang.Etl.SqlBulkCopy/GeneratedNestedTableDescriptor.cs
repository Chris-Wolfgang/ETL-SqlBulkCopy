using System;
using System.ComponentModel;

namespace Wolfgang.Etl.SqlBulkCopy;

/// <summary>
/// Describes one nested collection-to-table mapping of a <c>[BulkCopyable]</c>
/// type as pure data produced by the source generator. The child element type
/// is itself <c>[BulkCopyable]</c>, so its map is resolved at runtime through
/// its own registered descriptor — no reflection over either type.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class GeneratedNestedTableDescriptor
{
    /// <summary>
    /// Initializes a new <see cref="GeneratedNestedTableDescriptor"/>. Intended
    /// to be called only by generated code.
    /// </summary>
    /// <exception cref="ArgumentNullException">
    /// Thrown when <paramref name="propertyName"/> or <paramref name="childType"/>
    /// is <see langword="null"/>.
    /// </exception>
    public GeneratedNestedTableDescriptor(string propertyName, Type childType)
    {
        PropertyName = propertyName ?? throw new ArgumentNullException(nameof(propertyName));
        ChildType = childType ?? throw new ArgumentNullException(nameof(childType));
    }



    /// <summary>Gets the name of the collection property on the parent type.</summary>
    public string PropertyName { get; }



    /// <summary>Gets the element type of the collection (the child table type).</summary>
    public Type ChildType { get; }
}
