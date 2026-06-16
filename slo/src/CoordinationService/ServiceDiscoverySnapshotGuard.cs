namespace CoordinationService;

public sealed class ServiceDiscoverySnapshotGuard(string observerId)
{
    public int LastObservedEndpointCount { get; private set; }

    public void Observe(IEnumerable<ServiceEndpointPayload> endpoints)
    {
        var snapshot = endpoints.ToList();
        var duplicates = snapshot
            .GroupBy(endpoint => endpoint.Endpoint, StringComparer.Ordinal)
            .Where(group => group.Count() > 1)
            .Select(group => group.Key)
            .ToList();

        if (duplicates.Count > 0)
        {
            throw new CoordinationSloInvariantException(
                $"{observerId}: duplicate service endpoint(s): {string.Join(", ", duplicates)}.");
        }

        LastObservedEndpointCount = snapshot.Count;
    }
}
