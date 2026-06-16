using System.Threading.RateLimiting;
using System.Text;
using Internal;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;

namespace CoordinationService;

public sealed class SloCoordinationContext : ISloContext
{
    private const string NodeName = "slo-coordination";
    private const string SemaphoreName = "versioned-config";
    private const int ReaderCount = 4;
    private const string MutexName = "exclusive-lock";
    private const int MutexWorkerCount = 4;
    private const string ServiceDiscoverySemaphoreName = "service-discovery";
    private const int ServiceWorkerCount = 4;
    private const string ElectionSemaphoreName = "leader-election";
    private const int ElectionWorkerCount = 3;
    private const int RateLimitIntervalMs = 100;

    private static readonly ILogger Logger = ISloContext.Factory.CreateLogger<SloCoordinationContext>();

    public async Task Create(CreateConfig createConfig)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(createConfig.WriteTimeout));
        var client = new CoordinationClient(createConfig.ConnectionString);
        var nodePath = GetNodePath(createConfig.ConnectionString);

        await EnsureNode(client, nodePath, cts.Token);

        await using var session = client.CreateSession(
            nodePath,
            new SessionOptions { Description = "coordination-slo-create" });

        var semaphore = session.Semaphore(SemaphoreName);
        var initialPayload = CoordinationPayload.Encode(0, "bootstrap", DateTimeOffset.UtcNow);

        await RecreateSemaphore(semaphore, limit: 1, initialPayload, cts.Token);
        await RecreateSemaphore(
            session.Semaphore(ServiceDiscoverySemaphoreName),
            limit: ServiceWorkerCount,
            data: null,
            cts.Token);
        await RecreateSemaphore(
            session.Semaphore(ElectionSemaphoreName),
            limit: 1,
            data: null,
            cts.Token);

        Logger.LogInformation(
            "Coordination node {NodePath} and semaphores are ready: {ConfigSemaphore}, {DiscoverySemaphore}, {ElectionSemaphore}",
            nodePath,
            SemaphoreName,
            ServiceDiscoverySemaphoreName,
            ElectionSemaphoreName);
    }

    private static async Task RecreateSemaphore(
        Ydb.Sdk.Coordination.Semaphore semaphore,
        ulong limit,
        byte[]? data,
        CancellationToken token)
    {
        try
        {
            await semaphore.Delete(force: true, token);
        }
        catch (Exception ex)
        {
            Logger.LogDebug(ex, "Semaphore {SemaphoreName} was not deleted before setup", semaphore.Name);
        }

        await semaphore.Create(
            limit: limit,
            data: data,
            cancellationToken: token);

        if (data != null)
        {
            await semaphore.Update(data, token);
        }
    }

    public async Task Run(RunConfig runConfig)
    {
        await Create(new CreateConfig(
            runConfig.ConnectionString,
            runConfig.InitialDataCount,
            runConfig.WriteTimeout));

        var client = new CoordinationClient(runConfig.ConnectionString);
        var nodePath = GetNodePath(runConfig.ConnectionString);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(runConfig.Time));
        using var writeLimiter = NewLimiter(runConfig.WriteRps);
        using var readLimiter = NewLimiter(runConfig.ReadRps);
        using var mutexLimiter = NewLimiter(runConfig.WriteRps);
        using var serviceLimiter = NewLimiter(runConfig.WriteRps);
        using var electionLimiter = NewLimiter(Math.Max(1, runConfig.WriteRps / 2));
        var mutexGuard = new ExclusiveSectionGuard("mutex");

        var tasks = new List<Task>
        {
            RunWriter(client, nodePath, writeLimiter, runConfig.WriteTimeout, cts),
            RunConfigWatcher(client, nodePath, cts),
            RunServiceDiscoveryWatcher(client, nodePath, cts),
            RunElectionWatcher(client, nodePath, cts)
        };

        for (var i = 0; i < ReaderCount; i++)
        {
            tasks.Add(RunReader(client, nodePath, i, readLimiter, runConfig.ReadTimeout, cts));
        }

        for (var i = 0; i < MutexWorkerCount; i++)
        {
            tasks.Add(RunMutexWorker(client, nodePath, i, mutexLimiter, runConfig.WriteTimeout, mutexGuard, cts));
        }

        for (var i = 0; i < ServiceWorkerCount; i++)
        {
            tasks.Add(RunServiceWorker(client, nodePath, i, serviceLimiter, runConfig.WriteTimeout, cts));
        }

        for (var i = 0; i < ElectionWorkerCount; i++)
        {
            tasks.Add(RunElectionWorker(client, nodePath, i, electionLimiter, runConfig.WriteTimeout, cts));
        }

        try
        {
            Logger.LogInformation(
                "Started coordination SLO workload on {NodePath}/{SemaphoreName}",
                nodePath,
                SemaphoreName);

            await Task.WhenAll(tasks);
        }
        catch (CoordinationSloInvariantException)
        {
            await cts.CancelAsync();
            throw;
        }

        Logger.LogInformation("Coordination SLO workload finished");
    }

    private static async Task EnsureNode(CoordinationClient client, string nodePath, CancellationToken token)
    {
        var config = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(1),
            SessionGracePeriod = TimeSpan.FromSeconds(3),
            ReadConsistencyMode = ConsistencyMode.Strict,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };

        await client.CreateNode(nodePath, config, token);
        await client.AlterNode(nodePath, config, token);
    }

    private static async Task RunWriter(
        CoordinationClient client,
        string nodePath,
        RateLimiter writeLimiter,
        int writeTimeoutSeconds,
        CancellationTokenSource workloadCts)
    {
        await using var session = client.CreateSession(
            nodePath,
            new SessionOptions { Description = "coordination-slo-writer" });

        var semaphore = session.Semaphore(SemaphoreName);
        var version = 0L;

        while (!workloadCts.IsCancellationRequested)
        {
            try
            {
                using var lease = await writeLimiter.AcquireAsync(
                    cancellationToken: workloadCts.Token);

                if (!lease.IsAcquired)
                {
                    await Task.Delay(Random.Shared.Next(RateLimitIntervalMs / 2), workloadCts.Token);
                    continue;
                }

                using var opCts = CancellationTokenSource.CreateLinkedTokenSource(workloadCts.Token);
                opCts.CancelAfter(TimeSpan.FromSeconds(writeTimeoutSeconds));

                var nextVersion = Interlocked.Increment(ref version);
                var data = CoordinationPayload.Encode(
                    nextVersion,
                    "writer",
                    DateTimeOffset.UtcNow);

                await semaphore.Update(data, opCts.Token);

                Logger.LogDebug(
                    "Updated coordination payload to version {Version}",
                    nextVersion);
            }
            catch (OperationCanceledException) when (workloadCts.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Coordination writer update failed; continuing workload");
                await DelayAfterTransientFailure(workloadCts.Token);
            }
        }

        Logger.LogInformation("Coordination writer stopped");
    }

    private static async Task RunReader(
        CoordinationClient client,
        string nodePath,
        int readerId,
        RateLimiter readLimiter,
        int readTimeoutSeconds,
        CancellationTokenSource workloadCts)
    {
        await using var session = client.CreateSession(
            nodePath,
            new SessionOptions { Description = $"coordination-slo-reader-{readerId}" });

        var semaphore = session.Semaphore(SemaphoreName);
        var guard = new MonotonicVersionGuard($"reader-{readerId}");

        while (!workloadCts.IsCancellationRequested)
        {
            try
            {
                using var lease = await readLimiter.AcquireAsync(
                    cancellationToken: workloadCts.Token);

                if (!lease.IsAcquired)
                {
                    await Task.Delay(Random.Shared.Next(RateLimitIntervalMs / 2), workloadCts.Token);
                    continue;
                }

                using var opCts = CancellationTokenSource.CreateLinkedTokenSource(workloadCts.Token);
                opCts.CancelAfter(TimeSpan.FromSeconds(readTimeoutSeconds));

                var description = await semaphore.Describe(DescribeSemaphoreMode.DataOnly, opCts.Token);
                var payload = CoordinationPayload.Decode(description.Data);

                guard.Observe(payload);
            }
            catch (CoordinationSloInvariantException ex)
            {
                Logger.LogCritical(ex, "Coordination reader invariant failed");
                await workloadCts.CancelAsync();
                throw;
            }
            catch (OperationCanceledException) when (workloadCts.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Coordination reader {ReaderId} describe failed; continuing workload", readerId);
                await DelayAfterTransientFailure(workloadCts.Token);
            }
        }

        Logger.LogInformation(
            "Coordination reader {ReaderId} stopped at version {Version}",
            readerId,
            guard.LastObservedVersion);
    }

    private static async Task RunConfigWatcher(
        CoordinationClient client,
        string nodePath,
        CancellationTokenSource workloadCts)
    {
        var guard = new MonotonicVersionGuard("watcher");

        while (!workloadCts.IsCancellationRequested)
        {
            try
            {
                await using var session = client.CreateSession(
                    nodePath,
                    new SessionOptions { Description = "coordination-slo-config-watcher" });

                var semaphore = session.Semaphore(SemaphoreName);
                var watch = await semaphore.WatchSemaphore(
                    DescribeSemaphoreMode.DataOnly,
                    WatchSemaphoreMode.WatchData,
                    workloadCts.Token);

                guard.Observe(CoordinationPayload.Decode(watch.Initial.Data));

                await foreach (var description in watch.Updates.WithCancellation(workloadCts.Token))
                {
                    guard.Observe(CoordinationPayload.Decode(description.Data));
                }
            }
            catch (CoordinationSloInvariantException ex)
            {
                Logger.LogCritical(ex, "Coordination watcher invariant failed");
                await workloadCts.CancelAsync();
                throw;
            }
            catch (OperationCanceledException) when (workloadCts.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Coordination config watcher failed; recreating watch");
                await DelayAfterTransientFailure(workloadCts.Token);
            }
        }

        Logger.LogInformation(
            "Coordination config watcher stopped at version {Version}",
            guard.LastObservedVersion);
    }

    private static async Task RunMutexWorker(
        CoordinationClient client,
        string nodePath,
        int workerId,
        RateLimiter mutexLimiter,
        int operationTimeoutSeconds,
        ExclusiveSectionGuard guard,
        CancellationTokenSource workloadCts)
    {
        var ownerId = $"mutex-worker-{workerId}";

        while (!workloadCts.IsCancellationRequested)
        {
            try
            {
                await using var session = client.CreateSession(
                    nodePath,
                    new SessionOptions { Description = ownerId });
                var mutex = session.Mutex(MutexName);

                while (!workloadCts.IsCancellationRequested)
                {
                    using var permit = await mutexLimiter.AcquireAsync(cancellationToken: workloadCts.Token);
                    if (!permit.IsAcquired)
                    {
                        await Task.Delay(Random.Shared.Next(RateLimitIntervalMs / 2), workloadCts.Token);
                        continue;
                    }

                    using var opCts = CancellationTokenSource.CreateLinkedTokenSource(workloadCts.Token);
                    opCts.CancelAfter(TimeSpan.FromSeconds(operationTimeoutSeconds));

                    await using var lease = await mutex.Lock(opCts.Token);
                    using var exclusiveSection = guard.Enter(ownerId);
                    using var criticalCts = CancellationTokenSource.CreateLinkedTokenSource(
                        workloadCts.Token,
                        lease.Token);

                    await Task.Delay(Random.Shared.Next(25, 100), criticalCts.Token);
                }
            }
            catch (CoordinationSloInvariantException ex)
            {
                Logger.LogCritical(ex, "Coordination mutex invariant failed");
                await workloadCts.CancelAsync();
                throw;
            }
            catch (OperationCanceledException) when (workloadCts.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Coordination mutex worker {WorkerId} failed; recreating session", workerId);
                await DelayAfterTransientFailure(workloadCts.Token);
            }
        }

        Logger.LogInformation("Coordination mutex worker {WorkerId} stopped", workerId);
    }

    private static async Task RunServiceWorker(
        CoordinationClient client,
        string nodePath,
        int workerId,
        RateLimiter serviceLimiter,
        int operationTimeoutSeconds,
        CancellationTokenSource workloadCts)
    {
        var endpoint = $"http://coordination-slo-service-{workerId}:8080";
        var instanceId = $"service-worker-{workerId}";

        while (!workloadCts.IsCancellationRequested)
        {
            try
            {
                await using var session = client.CreateSession(
                    nodePath,
                    new SessionOptions { Description = instanceId });
                var semaphore = session.Semaphore(ServiceDiscoverySemaphoreName);

                while (!workloadCts.IsCancellationRequested)
                {
                    using var permit = await serviceLimiter.AcquireAsync(cancellationToken: workloadCts.Token);
                    if (!permit.IsAcquired)
                    {
                        await Task.Delay(Random.Shared.Next(RateLimitIntervalMs / 2), workloadCts.Token);
                        continue;
                    }

                    using var opCts = CancellationTokenSource.CreateLinkedTokenSource(workloadCts.Token);
                    opCts.CancelAfter(TimeSpan.FromSeconds(operationTimeoutSeconds));

                    var data = ServiceEndpointPayload.Encode(endpoint, instanceId, DateTimeOffset.UtcNow);
                    await using var lease = await semaphore.Acquire(
                        count: 1,
                        isEphemeral: true,
                        data: data,
                        timeout: null,
                        cancellationToken: opCts.Token);
                    using var registrationCts = CancellationTokenSource.CreateLinkedTokenSource(
                        workloadCts.Token,
                        lease.Token);

                    await Task.Delay(Random.Shared.Next(150, 400), registrationCts.Token);
                }
            }
            catch (OperationCanceledException) when (workloadCts.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Coordination service worker {WorkerId} failed; recreating registration", workerId);
                await DelayAfterTransientFailure(workloadCts.Token);
            }
        }

        Logger.LogInformation("Coordination service worker {WorkerId} stopped", workerId);
    }

    private static async Task RunServiceDiscoveryWatcher(
        CoordinationClient client,
        string nodePath,
        CancellationTokenSource workloadCts)
    {
        var guard = new ServiceDiscoverySnapshotGuard("service-discovery");

        while (!workloadCts.IsCancellationRequested)
        {
            try
            {
                await using var session = client.CreateSession(
                    nodePath,
                    new SessionOptions { Description = "coordination-slo-service-discovery-watcher" });
                var semaphore = session.Semaphore(ServiceDiscoverySemaphoreName);
                var watch = await semaphore.WatchSemaphore(
                    DescribeSemaphoreMode.WithOwners,
                    WatchSemaphoreMode.WatchOwners,
                    workloadCts.Token);

                guard.Observe(ReadServiceEndpoints(watch.Initial));

                await foreach (var description in watch.Updates.WithCancellation(workloadCts.Token))
                {
                    guard.Observe(ReadServiceEndpoints(description));
                }
            }
            catch (CoordinationSloInvariantException ex)
            {
                Logger.LogCritical(ex, "Coordination service discovery invariant failed");
                await workloadCts.CancelAsync();
                throw;
            }
            catch (OperationCanceledException) when (workloadCts.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Coordination service discovery watcher failed; recreating watch");
                await DelayAfterTransientFailure(workloadCts.Token);
            }
        }

        Logger.LogInformation(
            "Coordination service discovery watcher stopped at {EndpointCount} endpoint(s)",
            guard.LastObservedEndpointCount);
    }

    private static async Task RunElectionWorker(
        CoordinationClient client,
        string nodePath,
        int workerId,
        RateLimiter electionLimiter,
        int operationTimeoutSeconds,
        CancellationTokenSource workloadCts)
    {
        var workerName = $"election-worker-{workerId}";
        var data = Encoding.UTF8.GetBytes(workerName);

        while (!workloadCts.IsCancellationRequested)
        {
            try
            {
                await using var session = client.CreateSession(
                    nodePath,
                    new SessionOptions { Description = workerName });
                var election = session.Election(ElectionSemaphoreName);

                while (!workloadCts.IsCancellationRequested)
                {
                    using var permit = await electionLimiter.AcquireAsync(cancellationToken: workloadCts.Token);
                    if (!permit.IsAcquired)
                    {
                        await Task.Delay(Random.Shared.Next(RateLimitIntervalMs / 2), workloadCts.Token);
                        continue;
                    }

                    using var opCts = CancellationTokenSource.CreateLinkedTokenSource(workloadCts.Token);
                    opCts.CancelAfter(TimeSpan.FromSeconds(operationTimeoutSeconds));

                    await using var leadership = await election.Campaign(data, opCts.Token);
                    using var leadershipCts = CancellationTokenSource.CreateLinkedTokenSource(
                        workloadCts.Token,
                        session.Token());

                    await Task.Delay(Random.Shared.Next(150, 350), leadershipCts.Token);
                }
            }
            catch (OperationCanceledException) when (workloadCts.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Coordination election worker {WorkerId} failed; recreating campaign", workerId);
                await DelayAfterTransientFailure(workloadCts.Token);
            }
        }

        Logger.LogInformation("Coordination election worker {WorkerId} stopped", workerId);
    }

    private static async Task RunElectionWatcher(
        CoordinationClient client,
        string nodePath,
        CancellationTokenSource workloadCts)
    {
        var guard = new SingleLeaderSnapshotGuard("leader-election");

        while (!workloadCts.IsCancellationRequested)
        {
            try
            {
                await using var session = client.CreateSession(
                    nodePath,
                    new SessionOptions { Description = "coordination-slo-election-watcher" });
                var semaphore = session.Semaphore(ElectionSemaphoreName);
                var watch = await semaphore.WatchSemaphore(
                    DescribeSemaphoreMode.WithOwners,
                    WatchSemaphoreMode.WatchOwners,
                    workloadCts.Token);

                guard.Observe(ReadLeaders(watch.Initial));

                await foreach (var description in watch.Updates.WithCancellation(workloadCts.Token))
                {
                    guard.Observe(ReadLeaders(description));
                }
            }
            catch (CoordinationSloInvariantException ex)
            {
                Logger.LogCritical(ex, "Coordination election invariant failed");
                await workloadCts.CancelAsync();
                throw;
            }
            catch (OperationCanceledException) when (workloadCts.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Coordination election watcher failed; recreating watch");
                await DelayAfterTransientFailure(workloadCts.Token);
            }
        }

        Logger.LogInformation(
            "Coordination election watcher stopped at leader {Leader}",
            guard.LastObservedLeader?.WorkerId ?? "<none>");
    }

    private static IReadOnlyList<ServiceEndpointPayload> ReadServiceEndpoints(SemaphoreDescription description) =>
        description.OwnersList
            .Select(owner => ServiceEndpointPayload.Decode(owner.Data))
            .ToList();

    private static IReadOnlyList<LeaderSnapshot> ReadLeaders(SemaphoreDescription description) =>
        description.OwnersList
            .Select(owner => new LeaderSnapshot(
                Encoding.UTF8.GetString(owner.Data),
                owner.Id,
                owner.OrderId))
            .ToList();

    private static FixedWindowRateLimiter NewLimiter(int rps) =>
        new(new FixedWindowRateLimiterOptions
        {
            Window = TimeSpan.FromMilliseconds(RateLimitIntervalMs),
            PermitLimit = Math.Max(1, rps / (1000 / RateLimitIntervalMs)),
            QueueLimit = int.MaxValue
        });

    private static async Task DelayAfterTransientFailure(CancellationToken token)
    {
        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(250), token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
    }

    private static string GetNodePath(string connectionString)
    {
        var database = new YdbConnectionStringBuilder(connectionString).Database.TrimEnd('/');
        return $"{database}/{NodeName}";
    }
}
