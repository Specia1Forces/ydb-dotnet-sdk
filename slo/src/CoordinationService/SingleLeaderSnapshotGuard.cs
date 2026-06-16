namespace CoordinationService;

public sealed record LeaderSnapshot(string WorkerId, ulong SessionId, ulong OrderId);

public sealed class SingleLeaderSnapshotGuard(string observerId)
{
    public LeaderSnapshot? LastObservedLeader { get; private set; }

    public void Observe(IReadOnlyCollection<LeaderSnapshot> leaders)
    {
        if (leaders.Count > 1)
        {
            throw new CoordinationSloInvariantException(
                $"{observerId}: observed {leaders.Count} leaders at once.");
        }

        LastObservedLeader = leaders.FirstOrDefault();
    }
}

