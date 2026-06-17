namespace MongoBus.Internal.ClaimCheck;

/// <summary>
/// A read-only stream wrapper that throws once more than <c>maxBytes</c> have been read
/// from the underlying stream. Used to bound decompression output and defend against
/// decompression bombs, where a small compressed claim-check object expands to an
/// arbitrarily large payload.
/// </summary>
internal sealed class LimitedReadStream(Stream inner, long maxBytes) : Stream
{
    private long _read;

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => _read;
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var n = inner.Read(buffer, offset, count);
        Track(n);
        return n;
    }

    public override int Read(Span<byte> buffer)
    {
        var n = inner.Read(buffer);
        Track(n);
        return n;
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var n = await inner.ReadAsync(buffer.AsMemory(offset, count), cancellationToken);
        Track(n);
        return n;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var n = await inner.ReadAsync(buffer, cancellationToken);
        Track(n);
        return n;
    }

    private void Track(int n)
    {
        _read += n;
        if (_read > maxBytes)
            throw new InvalidDataException(
                $"Decompressed claim-check payload exceeded the maximum allowed size of {maxBytes} bytes.");
    }

    public override void Flush() => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            inner.Dispose();
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        await inner.DisposeAsync();
        await base.DisposeAsync();
    }
}
