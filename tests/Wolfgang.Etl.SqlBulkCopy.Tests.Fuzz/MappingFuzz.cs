// Continuous property-based fuzz over the type-mapping layer.
//
// The per-PR unit + conformance suites cover hand-picked shapes; this asks
// CsCheck to generate large numbers of parameterised inputs ([Trait
// "Category=Fuzz"]) and asserts per-input invariants that must hold across
// every generated value. Case count comes from CsCheck_Iterations (set by
// fuzz.yaml for the scheduled run; small by default for local iteration).
// A failing input auto-shrinks to a minimal counter-example.
//
// Refs #81.

using System;
using System.Text;
using CsCheck;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Fuzz;

public class MappingFuzz
{
    /// <summary>
    /// For any non-blank schema/table override, the bracket-quoting applied by
    /// <c>QualifiedTableName</c> must round-trip: un-quoting the result (undoubling
    /// <c>]]</c>) recovers the exact original identifiers. Catches any escaping
    /// bug that would let an identifier break out of its brackets.
    /// </summary>
    [Fact]
    [Trait("Category", "Fuzz")]
    public void QualifiedTableName_bracket_quoting_round_trips()
    {
        Gen.Select(Gen.String, Gen.String).Sample((schema, table) =>
        {
            // Blank overrides normalise to the type's attribute default rather
            // than the override, so they aren't part of this invariant.
            if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(table))
            {
                return true;
            }

            var map = TypeMap.Create(typeof(FuzzRecord), schema, table);
            var (recoveredSchema, recoveredTable) = Unbracket(map.QualifiedTableName);

            return string.Equals(recoveredSchema, schema, StringComparison.Ordinal)
                   && string.Equals(recoveredTable, table, StringComparison.Ordinal);
        });
    }



    /// <summary>
    /// For any record value, every mapped column's getter must return exactly
    /// what reflection reads from the same property. This exercises the compiled
    /// (or source-generated) getter path across arbitrary values.
    /// </summary>
    [Fact]
    [Trait("Category", "Fuzz")]
    public void ColumnMap_GetValue_matches_the_property_value()
    {
        var nullableInt = Gen.Int.Select(x => x % 7 == 0 ? (int?)null : x);

        Gen.Select(Gen.Int, Gen.String, nullableInt, Gen.Bool).Sample((a, b, c, d) =>
        {
            var instance = new FuzzRecord { A = a, B = b, C = c, D = d };
            var map = TypeMap.Create(typeof(FuzzRecord));

            foreach (var column in map.Columns)
            {
                var property = typeof(FuzzRecord).GetProperty(column.PropertyName)!;
                if (!Equals(property.GetValue(instance), column.GetValue(instance)))
                {
                    return false;
                }
            }

            return true;
        });
    }



    // Inverse of TypeMap's `[schema].[table]` quoting, where a literal ']' inside
    // an identifier is doubled to ']]'.
    private static (string Schema, string Table) Unbracket(string qualified)
    {
        var index = 0;
        var schema = ReadBracketedSegment(qualified, ref index);
        index++; // skip the '.' separator between the two segments
        var table = ReadBracketedSegment(qualified, ref index);
        return (schema, table);
    }



    private static string ReadBracketedSegment(string value, ref int index)
    {
        index++; // skip the opening '['
        var builder = new StringBuilder();

        while (index < value.Length)
        {
            if (value[index] == ']')
            {
                if (index + 1 < value.Length && value[index + 1] == ']')
                {
                    builder.Append(']');
                    index += 2;
                    continue;
                }

                index++; // consume the closing ']'
                break;
            }

            builder.Append(value[index]);
            index++;
        }

        return builder.ToString();
    }
}
