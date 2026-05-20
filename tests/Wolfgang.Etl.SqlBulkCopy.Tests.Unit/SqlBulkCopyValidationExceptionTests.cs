using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using Wolfgang.Etl.SqlBulkCopy.Tests.Unit.TestModels;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Unit;

public class SqlBulkCopyValidationExceptionTests
{
    [Fact]
    public void Parameterless_constructor_sets_empty_ValidationResults_and_null_Item()
    {
        var ex = new SqlBulkCopyValidationException();

        Assert.Null(ex.Item);
        Assert.Empty(ex.ValidationResults);
    }



    [Fact]
    public void Message_constructor_sets_message_and_empty_ValidationResults()
    {
        var ex = new SqlBulkCopyValidationException("custom message");

        Assert.Equal("custom message", ex.Message);
        Assert.Null(ex.Item);
        Assert.Empty(ex.ValidationResults);
    }



    [Fact]
    public void Message_and_inner_constructor_sets_both()
    {
        var inner = new InvalidOperationException("inner");

        var ex = new SqlBulkCopyValidationException("outer", inner);

        Assert.Equal("outer", ex.Message);
        Assert.Same(inner, ex.InnerException);
        Assert.Null(ex.Item);
        Assert.Empty(ex.ValidationResults);
    }



    [Fact]
    public void Item_constructor_sets_Item_and_ValidationResults()
    {
        var item = new ValidatableRecord { Id = 1, Name = "", Quantity = 5 };
        var results = new List<ValidationResult>
        {
            new ValidationResult("Name is required", new[] { "Name" })
        };

        var ex = new SqlBulkCopyValidationException(item, results);

        Assert.Same(item, ex.Item);
        Assert.Single(ex.ValidationResults);
        Assert.Contains("ValidatableRecord", ex.Message, StringComparison.Ordinal);
        Assert.Contains("1 errors", ex.Message, StringComparison.Ordinal);
    }



    [Fact]
    public void Item_constructor_when_item_is_null_throws_ArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>
        (
            () => new SqlBulkCopyValidationException(null!, new List<ValidationResult>())
        );
    }



    [Fact]
    public void Item_constructor_when_validationResults_is_null_throws_ArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>
        (
            () => new SqlBulkCopyValidationException(new ValidatableRecord(), null!)
        );
    }
}
