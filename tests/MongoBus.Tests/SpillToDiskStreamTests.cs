using FluentAssertions;
using MongoBus.Internal.ClaimCheck;
using Xunit;

namespace MongoBus.Tests;

public sealed class SpillToDiskStreamTests : IDisposable
{
    private const int MemoryLimit = 64;
    private readonly DirectoryInfo _spillDirectory = Directory.CreateTempSubdirectory("mongobus-spill-tests-");

    public void Dispose() => _spillDirectory.Delete(recursive: true);

    [Fact]
    public async Task Should_Keep_Payload_In_Memory_While_It_Fits_The_Limit()
    {
        var payload = Bytes(MemoryLimit);
        await using var stream = new SpillToDiskStream(MemoryLimit, _spillDirectory.FullName);

        await stream.WriteAsync(payload);

        _spillDirectory.GetFiles().Should().BeEmpty();
        (await ReadAllAsync(stream)).Should().Equal(payload);
    }

    [Fact]
    public async Task Should_Move_Payload_To_A_Temp_File_Once_It_Exceeds_The_Limit()
    {
        var payload = Bytes(MemoryLimit * 3);
        await using var stream = new SpillToDiskStream(MemoryLimit, _spillDirectory.FullName);

        foreach (var chunk in payload.Chunk(MemoryLimit / 2))
            await stream.WriteAsync(chunk);

        _spillDirectory.GetFiles().Should().ContainSingle();
        (await ReadAllAsync(stream)).Should().Equal(payload);
    }

    [Fact]
    public async Task Should_Move_Payload_To_A_Temp_File_When_Written_Synchronously_Past_The_Limit()
    {
        var payload = Bytes(MemoryLimit * 3);
        await using var stream = new SpillToDiskStream(MemoryLimit, _spillDirectory.FullName);

        foreach (var chunk in payload.Chunk(MemoryLimit / 2))
            stream.Write(chunk);

        _spillDirectory.GetFiles().Should().ContainSingle();
        (await ReadAllAsync(stream)).Should().Equal(payload);
    }

    [Fact]
    public async Task Should_Delete_The_Temp_File_When_Disposed()
    {
        var stream = new SpillToDiskStream(MemoryLimit, _spillDirectory.FullName);
        await stream.WriteAsync(Bytes(MemoryLimit + 1));

        await stream.DisposeAsync();

        _spillDirectory.GetFiles().Should().BeEmpty();
    }

    private static byte[] Bytes(int count) => Enumerable.Range(0, count).Select(i => (byte)i).ToArray();

    private static async Task<byte[]> ReadAllAsync(Stream stream)
    {
        stream.Position = 0;
        using var copy = new MemoryStream();
        await stream.CopyToAsync(copy);
        return copy.ToArray();
    }
}
