using System.Threading;

namespace CoordinationService;

public sealed class ExclusiveSectionGuard(string guardId)
{
    private int _ownersInside;

    public IDisposable Enter(string ownerId)
    {
        var ownersInside = Interlocked.Increment(ref _ownersInside);
        if (ownersInside > 1)
        {
            Interlocked.Decrement(ref _ownersInside);
            throw new CoordinationSloInvariantException(
                $"{guardId}: concurrent owner {ownerId} entered exclusive section ({ownersInside} owners).");
        }

        return new ExitHandle(this);
    }

    private sealed class ExitHandle(ExclusiveSectionGuard guard) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                Interlocked.Decrement(ref guard._ownersInside);
            }
        }
    }
}

