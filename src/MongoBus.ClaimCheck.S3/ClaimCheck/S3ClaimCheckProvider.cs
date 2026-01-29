using Amazon;
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

    public S3ClaimCheckProvider(S3ClaimCheckOptions options)
    {
        _options = options;
        var config = new AmazonS3Config
        {
            ServiceURL = options.ServiceUrl,
            ForcePathStyle = options.ForcePathStyle,
            RegionEndpoint = string.IsNullOrWhiteSpace(options.Region) ? null : RegionEndpoint.GetBySystemName(options.Region)
        };

        _client = new AmazonS3Client(options.AccessKey, options.SecretKey, config);
    }

    public string Name => _options.ProviderName;

    public async Task<ClaimCheckReference> PutAsync(ClaimCheckWriteRequest request, CancellationToken ct)
    {
        var key = BuildKey();

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

        var length = request.Length ?? (request.Data.CanSeek ? request.Data.Length : 0);
        return new ClaimCheckReference(Name, _options.BucketName, key, length, request.ContentType, request.Metadata);
    }

    public async Task<Stream> OpenReadAsync(ClaimCheckReference reference, CancellationToken ct)
    {
        var response = await _client.GetObjectAsync(reference.Container, reference.Key, ct);
        return new ResponseStream(response);
    }

    public async Task DeleteAsync(ClaimCheckReference reference, CancellationToken ct)
    {
        await _client.DeleteObjectAsync(reference.Container, reference.Key, ct);
    }

    public async IAsyncEnumerable<ClaimCheckReference> ListAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var request = new ListObjectsV2Request
        {
            BucketName = _options.BucketName,
            Prefix = _options.KeyPrefix
        };

        ListObjectsV2Response response;
        do
        {
            response = await _client.ListObjectsV2Async(request, ct);
            foreach (var s3Object in response.S3Objects)
            {
                // To get metadata, we'd need to call GetObjectMetadata for each object.
                // S3 ListObjects doesn't return user-defined metadata.
                // However, we have LastModified from the list result.

                yield return new ClaimCheckReference(
                    Provider: Name,
                    Container: _options.BucketName,
                    Key: s3Object.Key,
                    Length: s3Object.Size,
                    CreatedAt: s3Object.LastModified.ToUniversalTime());
            }

            request.ContinuationToken = response.NextContinuationToken;
        } while (response.IsTruncated);
    }

    private string BuildKey()
    {
        var prefix = string.IsNullOrWhiteSpace(_options.KeyPrefix) ? "" : _options.KeyPrefix!.TrimEnd('/') + "/";
        return $"{prefix}{Guid.NewGuid():N}";
    }

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
