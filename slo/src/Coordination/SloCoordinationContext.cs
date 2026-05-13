using System.Text;
using Internal;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;

namespace CoordinationSlo;

public sealed class SloCoordinationContext : ISloContext
{
    private const string NodePath = "/local/slo-coordination";
    private const string MutexName = "slo-mutex";
    private const string ServicesName = "slo-services";
    private const string ConfigName = "slo-config";

    private static readonly Encoding Utf8 = Encoding.UTF8;
    private static readonly ILogger Logger = ISloContext.Factory.CreateLogger<SloCoordinationContext>();

    private int _activeMutexOwners;
    private long _configVersion;

    public async Task Create(CreateConfig createConfig)
    {
        var client = new CoordinationClient(createConfig.ConnectionString);
        await EnsureNode(client, CancellationToken.None);
        await EnsureConfigSemaphore(client, CancellationToken.None);
    }

    public async Task Run(RunConfig runConfig)
    {
        var client = new CoordinationClient(runConfig.ConnectionString);
        await EnsureNode(client, CancellationToken.None);
        await EnsureConfigSemaphore(client, CancellationToken.None);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(runConfig.Time));

        var tasks = new List<Task>
        {
            RunConfigPublisher(client, cts.Token),
            RunConfigWatcher(client, cts.Token),
            RunServiceWatcher(client, cts.Token)
        };

        for (var i = 0; i < 4; i++)
        {
            tasks.Add(RunMutexWorker(client, i, cts.Token));
        }

        for (var i = 0; i < 3; i++)
        {
            tasks.Add(RunServiceWorker(client, $"worker-{i}:808{i}", cts.Token));
        }

        Logger.LogInformation("Started coordination chaos workload");

        try
        {
            await Task.WhenAll(tasks);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
        }

        Logger.LogInformation("Coordination chaos workload finished");
    }

    private static NodeConfig NodeConfig() => new()
    {
        SelfCheckPeriod = TimeSpan.FromSeconds(1),
        SessionGracePeriod = TimeSpan.FromSeconds(10),
        ReadConsistencyMode = ConsistencyMode.Relaxed,
        AttachConsistencyMode = ConsistencyMode.Relaxed,
        RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
    };

    private static async Task EnsureNode(CoordinationClient client, CancellationToken token)
    {
        await Retry(
            async () => await client.CreateNode(NodePath, NodeConfig(), token),
            "create coordination node",
            token);
    }

    private static async Task EnsureConfigSemaphore(CoordinationClient client, CancellationToken token)
    {
        var session = client.CreateSession(NodePath);
        var semaphore = session.Semaphore(ConfigName);
        try
        {
            await Retry(
                async () => await semaphore.Create(1, Utf8.GetBytes("version:0"), token),
                "create config semaphore",
                token);
        }
        catch (YdbException ex)
        {
            Logger.LogWarning(ex, "Config semaphore already exists or cannot be created now");
        }
        finally
        {
            await CloseQuietly(session);
        }
    }

    private async Task RunMutexWorker(CoordinationClient client, int workerId, CancellationToken runToken)
    {
        while (!runToken.IsCancellationRequested)
        {
            var session = client.CreateSession(NodePath);
            var mutex = session.Mutex(MutexName);

            try
            {
                while (!runToken.IsCancellationRequested && !session.Token().IsCancellationRequested)
                {
                    Lease? lease = null;
                    var entered = false;

                    try
                    {
                        lease = await mutex.Lock(runToken);
                        var active = Interlocked.Increment(ref _activeMutexOwners);
                        entered = true;

                        if (active != 1)
                        {
                            throw new InvalidOperationException(
                                $"FAILED SLO TEST: mutex has {active} concurrent owners");
                        }

                        Logger.LogDebug("Mutex worker {WorkerId} acquired lock", workerId);

                        using var workCts = CancellationTokenSource.CreateLinkedTokenSource(
                            runToken,
                            session.Token(),
                            lease.Token);
                        await Task.Delay(TimeSpan.FromMilliseconds(200), workCts.Token);
                    }
                    catch (OperationCanceledException) when (
                        runToken.IsCancellationRequested || session.Token().IsCancellationRequested)
                    {
                        break;
                    }
                    catch (YdbException ex)
                    {
                        Logger.LogWarning(ex, "Mutex worker {WorkerId} operation failed", workerId);
                        if (session.Token().IsCancellationRequested)
                            break;

                        await DelayBeforeRetry(runToken);
                    }
                    finally
                    {
                        if (entered)
                        {
                            Interlocked.Decrement(ref _activeMutexOwners);
                        }

                        await ReleaseQuietly(lease, session, runToken);
                    }

                    await DelayBeforeRetry(runToken);
                }
            }
            finally
            {
                await CloseQuietly(session);
            }
        }
    }

    private static async Task RunServiceWorker(
        CoordinationClient client,
        string endpoint,
        CancellationToken runToken)
    {
        var payload = Utf8.GetBytes(endpoint);

        while (!runToken.IsCancellationRequested)
        {
            var session = client.CreateSession(NodePath);
            var semaphore = session.Semaphore(ServicesName);

            try
            {
                while (!runToken.IsCancellationRequested && !session.Token().IsCancellationRequested)
                {
                    Lease? lease = null;
                    try
                    {
                        lease = await semaphore.Acquire(1, true, payload, null, runToken);
                        Logger.LogDebug("Service endpoint {Endpoint} registered", endpoint);

                        using var holdCts = CancellationTokenSource.CreateLinkedTokenSource(
                            runToken,
                            session.Token(),
                            lease.Token);
                        await Task.Delay(TimeSpan.FromMilliseconds(750), holdCts.Token);
                    }
                    catch (OperationCanceledException) when (
                        runToken.IsCancellationRequested || session.Token().IsCancellationRequested)
                    {
                        break;
                    }
                    catch (YdbException ex)
                    {
                        Logger.LogWarning(ex, "Service endpoint {Endpoint} operation failed", endpoint);
                        if (session.Token().IsCancellationRequested)
                            break;

                        await DelayBeforeRetry(runToken);
                    }
                    finally
                    {
                        await ReleaseQuietly(lease, session, runToken);
                    }

                    await DelayBeforeRetry(runToken);
                }
            }
            finally
            {
                await CloseQuietly(session);
            }
        }
    }

    private static async Task RunServiceWatcher(CoordinationClient client, CancellationToken runToken)
    {
        while (!runToken.IsCancellationRequested)
        {
            var session = client.CreateSession(NodePath);
            var semaphore = session.Semaphore(ServicesName);

            try
            {
                var watch = await semaphore.WatchSemaphore(
                    DescribeSemaphoreMode.WithOwners,
                    WatchSemaphoreMode.WatchOwners,
                    runToken);

                ValidateServiceOwners(watch.Initial);

                await foreach (var description in watch.Updates.WithCancellation(runToken))
                {
                    ValidateServiceOwners(description);
                }
            }
            catch (OperationCanceledException) when (
                runToken.IsCancellationRequested || session.Token().IsCancellationRequested)
            {
            }
            catch (YdbException ex)
            {
                Logger.LogWarning(ex, "Service watcher failed");
                await DelayBeforeRetry(runToken);
            }
            finally
            {
                await CloseQuietly(session);
            }
        }
    }

    private async Task RunConfigPublisher(CoordinationClient client, CancellationToken runToken)
    {
        while (!runToken.IsCancellationRequested)
        {
            var session = client.CreateSession(NodePath);
            var semaphore = session.Semaphore(ConfigName);

            try
            {
                while (!runToken.IsCancellationRequested && !session.Token().IsCancellationRequested)
                {
                    var version = Interlocked.Increment(ref _configVersion);
                    var payload = Utf8.GetBytes($"version:{version}");

                    await semaphore.Update(payload, runToken);
                    Logger.LogDebug("Published config version {Version}", version);

                    await Task.Delay(TimeSpan.FromMilliseconds(500), runToken);
                }
            }
            catch (OperationCanceledException) when (
                runToken.IsCancellationRequested || session.Token().IsCancellationRequested)
            {
            }
            catch (YdbException ex)
            {
                Logger.LogWarning(ex, "Config publisher failed");
                await DelayBeforeRetry(runToken);
            }
            finally
            {
                await CloseQuietly(session);
            }
        }
    }

    private static async Task RunConfigWatcher(CoordinationClient client, CancellationToken runToken)
    {
        var lastVersion = 0L;

        while (!runToken.IsCancellationRequested)
        {
            var session = client.CreateSession(NodePath);
            var semaphore = session.Semaphore(ConfigName);

            try
            {
                var watch = await semaphore.WatchSemaphore(
                    DescribeSemaphoreMode.DataOnly,
                    WatchSemaphoreMode.WatchData,
                    runToken);

                lastVersion = ValidateConfigVersion(watch.Initial.Data, lastVersion);

                await foreach (var description in watch.Updates.WithCancellation(runToken))
                {
                    lastVersion = ValidateConfigVersion(description.Data, lastVersion);
                }
            }
            catch (OperationCanceledException) when (
                runToken.IsCancellationRequested || session.Token().IsCancellationRequested)
            {
            }
            catch (YdbException ex)
            {
                Logger.LogWarning(ex, "Config watcher failed");
                await DelayBeforeRetry(runToken);
            }
            finally
            {
                await CloseQuietly(session);
            }
        }
    }

    private static void ValidateServiceOwners(SemaphoreDescription description)
    {
        var endpoints = description.OwnersList
            .Select(owner => Utf8.GetString(owner.Data))
            .Where(endpoint => !string.IsNullOrWhiteSpace(endpoint))
            .ToArray();

        if (endpoints.Length != endpoints.Distinct(StringComparer.Ordinal).Count())
        {
            throw new InvalidOperationException(
                "FAILED SLO TEST: service discovery returned duplicate endpoints");
        }
    }

    private static long ValidateConfigVersion(byte[] data, long lastVersion)
    {
        if (data.Length == 0)
            return lastVersion;

        var payload = Utf8.GetString(data);
        if (!payload.StartsWith("version:", StringComparison.Ordinal))
            return lastVersion;

        if (!long.TryParse(payload["version:".Length..], out var version))
            throw new InvalidOperationException($"FAILED SLO TEST: invalid config payload {payload}");

        if (version < lastVersion)
        {
            throw new InvalidOperationException(
                $"FAILED SLO TEST: config version moved backwards from {lastVersion} to {version}");
        }

        return version;
    }

    private static async Task ReleaseQuietly(
        Lease? lease,
        CoordinationSession session,
        CancellationToken runToken)
    {
        if (lease == null)
            return;

        if (session.Token().IsCancellationRequested || runToken.IsCancellationRequested)
            return;

        try
        {
            await lease.Release(CancellationToken.None);
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Failed to release coordination lease");
        }
    }

    private static async Task CloseQuietly(CoordinationSession session)
    {
        try
        {
            await session.Close();
        }
        catch (Exception ex)
        {
            Logger.LogWarning(ex, "Failed to close coordination session");
        }
    }

    private static async Task DelayBeforeRetry(CancellationToken token)
    {
        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(Random.Shared.Next(100, 300)), token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
    }

    private static async Task Retry(
        Func<Task> action,
        string operation,
        CancellationToken token,
        int attempts = 5)
    {
        for (var attempt = 1; attempt <= attempts; attempt++)
        {
            try
            {
                await action();
                return;
            }
            catch (Exception ex) when (attempt < attempts)
            {
                Logger.LogWarning(ex, "Failed to {Operation}, attempt {Attempt}/{Attempts}",
                    operation, attempt, attempts);
                await Task.Delay(TimeSpan.FromSeconds(attempt), token);
            }
        }
    }
}
