namespace MongoBus.Utils;

public static class EndpointNameHelper
{
    public static string FromConsumerType(Type consumerType)
    {
        var name = consumerType.Name;
        if (name.EndsWith("Consumer", StringComparison.OrdinalIgnoreCase))
            name = name[..^"Consumer".Length];
        return ToKebabCase(name);
    }

    private static string ToKebabCase(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return input;
        var chars = new List<char>(input.Length * 2);
        for (var i = 0; i < input.Length; i++)
        {
            var c = input[i];
            if (i > 0 && char.IsUpper(c) && (char.IsLower(input[i - 1]) || char.IsDigit(input[i - 1])))
                chars.Add('-');
            chars.Add(char.ToLowerInvariant(c));
        }
        return new string(chars.ToArray());
    }
}
