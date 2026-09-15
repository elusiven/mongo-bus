using System.Diagnostics.CodeAnalysis;

namespace MongoBus.Internal.ClaimCheck;

/// <summary>
/// A readable, seekable buffer that keeps what is written to it in memory until it would grow past
/// <c>memoryLimit</c> bytes, then moves it to a temporary file in <c>directory</c>. The file is deleted
/// when the stream is disposed.
/// </summary>
internal sealed class SpillToDiskStream(long memoryLimit, string directory) : Stream
{
    private const int FileBufferSize = 81920;

    private Stream _buffer = new MemoryStream();

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => true;
    public override long Length => _buffer.Length;

    public override long Position
    {
        get => _buffer.Position;
        set => _buffer.Position = value;
    }

    public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        if (WouldOutgrowMemory(buffer.Length, out var memory))
            _buffer = SpillToDisk(memory);

        _buffer.Write(buffer);
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        if (WouldOutgrowMemory(buffer.Length, out var memory))
            _buffer = await SpillToDiskAsync(memory, cancellationToken);

        await _buffer.WriteAsync(buffer, cancellationToken);
    }

    public override int Read(byte[] buffer, int offset, int count) => _buffer.Read(buffer, offset, count);

    public override int Read(Span<byte> buffer) => _buffer.Read(buffer);

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        _buffer.ReadAsync(buffer, offset, count, cancellationToken);

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
        _buffer.ReadAsync(buffer, cancellationToken);

    public override long Seek(long offset, SeekOrigin origin) => _buffer.Seek(offset, origin);

    public override void SetLength(long value) => _buffer.SetLength(value);

    public override void Flush() => _buffer.Flush();

    public override Task FlushAsync(CancellationToken cancellationToken) => _buffer.FlushAsync(cancellationToken);

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            _buffer.Dispose();
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        await _buffer.DisposeAsync();
        await base.DisposeAsync();
    }

    private bool WouldOutgrowMemory(int count, [NotNullWhen(true)] out MemoryStream? memory)
    {
        memory = _buffer as MemoryStream;
        return memory is not null && memory.Position + count > memoryLimit;
    }

    private FileStream SpillToDisk(MemoryStream memory)
    {
        var file = CreateTempFile();
        memory.WriteTo(file);
        file.Position = memory.Position;
        memory.Dispose();
        return file;
    }

    private async Task<FileStream> SpillToDiskAsync(MemoryStream memory, CancellationToken ct)
    {
        var file = CreateTempFile();
        await file.WriteAsync(memory.GetBuffer().AsMemory(0, (int)memory.Length), ct);
        file.Position = memory.Position;
        await memory.DisposeAsync();
        return file;
    }

    private FileStream CreateTempFile() =>
        new(
            Path.Combine(directory, $"mongobus-claimcheck-{Guid.NewGuid():N}.json"),
            FileMode.CreateNew,
            FileAccess.ReadWrite,
            FileShare.None,
            FileBufferSize,
            FileOptions.Asynchronous | FileOptions.DeleteOnClose);
}
