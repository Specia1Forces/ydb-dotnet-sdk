using System.Text.Json;

namespace CoordinationService;

public sealed record ServiceEndpointPayload(string Endpoint, string InstanceId, DateTimeOffset UpdatedAt)
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static byte[] Encode(string endpoint, string instanceId, DateTimeOffset updatedAt) =>
        JsonSerializer.SerializeToUtf8Bytes(new ServiceEndpointPayload(endpoint, instanceId, updatedAt), JsonOptions);

    public static ServiceEndpointPayload Decode(byte[] data)
    {
        var payload = JsonSerializer.Deserialize<ServiceEndpointPayload>(data, JsonOptions);
        if (payload == null || string.IsNullOrWhiteSpace(payload.Endpoint))
        {
            throw new CoordinationSloInvariantException("Service discovery owner payload is empty or invalid JSON.");
        }

        return payload;
    }
}

