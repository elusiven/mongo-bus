namespace MongoBus.Internal.ClaimCheck;

internal static class ClaimCheckConstants
{
    public const string ContentType = "application/vnd.mongobus.claim-check+json";
    public const string DefaultObjectContentType = "application/json";
    public const string DefaultStreamContentType = "application/octet-stream";
    public const string CreatedAtMetadataKey = "x-mongobus-created-at";
}
