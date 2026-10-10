using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using Wolfgang.Etl.SqlBulkCopy.Tests.Integration.Fixtures;
using Xunit;

namespace Wolfgang.Etl.SqlBulkCopy.Tests.Integration;

/// <summary>
/// Pins which container-start failures <see cref="SqlServerFixture"/> treats as
/// "Docker is unavailable, skip" and which propagate as real failures. These run
/// without a container: the classification is a pure function of the exception.
/// </summary>
public class SqlServerFixtureTests
{
    [Fact]
    public void IsDockerUnavailable_when_exception_is_IOException_returns_true()
    {
        Assert.True(SqlServerFixture.IsDockerUnavailable(new IOException("pipe not found")));
    }



    [Fact]
    public void IsDockerUnavailable_when_AggregateException_wraps_SocketException_returns_true()
    {
        var ex = new AggregateException(new SocketException((int)SocketError.ConnectionRefused));

        Assert.True(SqlServerFixture.IsDockerUnavailable(ex));
    }



    [Fact]
    public void IsDockerUnavailable_when_exception_is_PlatformNotSupportedException_returns_true()
    {
        Assert.True(SqlServerFixture.IsDockerUnavailable(new PlatformNotSupportedException()));
    }



    [Fact]
    public void IsDockerUnavailable_when_exception_is_named_DockerUnavailableException_returns_true()
    {
        Assert.True(SqlServerFixture.IsDockerUnavailable(new DockerUnavailableException()));
    }



    [Fact]
    public void IsDockerUnavailable_when_exception_comes_from_Docker_DotNet_returns_true()
    {
        var ex = new Docker.DotNet.DockerApiException(HttpStatusCode.InternalServerError, "daemon error");

        Assert.True(SqlServerFixture.IsDockerUnavailable(ex));
    }



    [Fact]
    public void IsDockerUnavailable_when_ArgumentException_names_DockerEndpointAuthConfig_returns_true()
    {
        var ex = new ArgumentException("Docker is not installed.", "DockerEndpointAuthConfig");

        Assert.True(SqlServerFixture.IsDockerUnavailable(ex));
    }



    [Fact]
    public void IsDockerUnavailable_when_ArgumentException_names_another_parameter_returns_false()
    {
        var ex = new ArgumentException("Bad image tag.", "image");

        Assert.False(SqlServerFixture.IsDockerUnavailable(ex));
    }



    [Fact]
    public void IsDockerUnavailable_when_AggregateException_has_no_inner_exception_returns_false()
    {
        Assert.False(SqlServerFixture.IsDockerUnavailable(new AggregateException()));
    }



    [Fact]
    public void IsDockerUnavailable_when_exception_is_unrelated_returns_false()
    {
        Assert.False(SqlServerFixture.IsDockerUnavailable(new InvalidOperationException("fixture bug")));
    }



    // Stands in for Testcontainers' DotNet.Testcontainers.Builders.DockerUnavailableException,
    // which the fixture matches by simple type name.
    private sealed class DockerUnavailableException : Exception
    {
    }
}
