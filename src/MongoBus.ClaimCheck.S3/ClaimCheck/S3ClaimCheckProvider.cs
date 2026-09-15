using Amazon.S3;
using Amazon.S3.Model;
using MongoBus.Abstractions;
using MongoBus.DependencyInjection;
using MongoBus.Internal.ClaimCheck;
using MongoBus.Models;

namespace MongoBus.ClaimCheck;

public sealed class S3ClaimCheckProvider : IClaimCheckProvider
{
    private readonly S3ClaimCheckOptions _options;
    private readonly IAmazonS3 _client;
    private readonly string? _keyPrefix;

    public S3ClaimCheckProvider(S3ClaimCheckOptions options)
    {
        _options = options;
        _client = new AmazonS3Client(options.AccessKey, options.SecretKey, S3ClientConfiguration.Create(options));
        _keyPrefix = string.IsNullOrWhiteSpace(options.KeyPrefix) ? null : options.KeyPrefix.TrimEnd('/') + "/";
    }

    public string Name => _options.ProviderName;

    public async Task<ClaimCheckReference> PutAsync(ClaimCheckWriteRequest request, CancellationToken ct)
    {
        var key = BuildKey();
        // Measured before uploading because the SDK closes the input stream once the upload completes.
        var length = request.Length ?? (request.Data.CanSeek ? request.Data.Length : 0);

        var put = new PutObjectRequest
        {
            BucketName = _options.BucketName,
            Key = key,
            InputStream = request.Data,
            ContentType = request.ContentType
        };

        if (request.Metadata is not null)
        {
            foreach (var kv in request.Metadata)
                put.Metadata[kv.Key] = kv.Value;
        }

        await _client.PutObjectAsync(put, ct);

        return new ClaimCheckReference(Name, _options.BucketName, key, length, request.ContentType, request.Metadata);
    }

    public async Task<Stream> OpenReadAsync(ClaimCheckReference reference, CancellationToken ct)
    {
        // Always read from the configured bucket, never the bucket named in the
        // (untrusted) reference, so a forged message cannot coerce reads from an
        // arbitrary bucket the configured credentials can access.
        var response = await _client.GetObjectAsync(_options.BucketName, reference.Key, ct);
        return new ResponseStream(response);
    }

    public async Task DeleteAsync(ClaimCheckReference reference, CancellationToken ct)
    {
        await _client.DeleteObjectAsync(_options.BucketName, reference.Key, ct);
    }

    public async IAsyncEnumerable<ClaimCheckReference> ListAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var request = new ListObjectsV2Request
        {
            BucketName = _options.BucketName,
            Prefix = _keyPrefix
        };

        ListObjectsV2Response response;
        do
        {
            response = await _client.ListObjectsV2Async(request, ct);
            foreach (var s3Object in response.S3Objects ?? [])
            {
                if (!IsNamedLikeClaimCheck(s3Object.Key))
                    continue;

                yield return new ClaimCheckReference(
                    Provider: Name,
                    Container: _options.BucketName,
                    Key: s3Object.Key,
                    Length: s3Object.Size ?? 0,
                    CreatedAt: s3Object.LastModified?.ToUniversalTime());
            }

            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated == true);
    }

    private string BuildKey() => $"{_keyPrefix}{Guid.NewGuid():N}";

    // The bucket may hold objects other applications wrote. ListObjectsV2 returns no user metadata, and fetching it
    // would cost one request per object, so MongoBus payloads are recognised by the key BuildKey gives them instead.
    private bool IsNamedLikeClaimCheck(string key) =>
        Guid.TryParseExact(key.AsSpan(_keyPrefix?.Length ?? 0), "N", out _);

    private sealed class ResponseStream : Stream
    {
        private readonly GetObjectResponse _response;
        private readonly Stream _inner;

        public ResponseStream(GetObjectResponse response)
        {
            _response = response;
            _inner = response.ResponseStream;
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => _inner.Length;
        public override long Position { get => _inner.Position; set => _inner.Position = value; }
        public override void Flush() => _inner.Flush();
        public override Task FlushAsync(CancellationToken cancellationToken) => _inner.FlushAsync(cancellationToken);
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
        public override int Read(Span<byte> buffer) => _inner.Read(buffer);
        public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => _inner.ReadAsync(buffer, offset, count, cancellationToken);
        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => _inner.ReadAsync(buffer, cancellationToken);
        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
        public override void SetLength(long value) => _inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
        public override void Write(ReadOnlySpan<byte> buffer) => _inner.Write(buffer);
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => _inner.WriteAsync(buffer, offset, count, cancellationToken);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => _inner.WriteAsync(buffer, cancellationToken);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _inner.Dispose();
                _response.Dispose();
            }
            base.Dispose(disposing);
        }

        public override async ValueTask DisposeAsync()
        {
            await _inner.DisposeAsync();
            _response.Dispose();
            await base.DisposeAsync();
        }
    }
}
