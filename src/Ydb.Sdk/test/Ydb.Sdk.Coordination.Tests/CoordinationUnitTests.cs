using System.Collections.Concurrent;
using System.Threading.Channels;
using Grpc.Core;
using Moq;
using Xunit;
using Ydb.Coordination;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.RequestRegistry;
using Ydb.Sdk.Coordination.Watcher;

namespace Ydb.Sdk.Coordination.Tests;

public class CoordinationUnitTests
{
    private const string NodePath = "/local/coordination-unit-test";

    [Fact]
    public async Task AcquireSemaphore_WhenSessionStarted_SendsSessionStartAndAcquireRequest()
    {
        await using var stream = new TestCoordinationStream();
        var driver = CreateDriver(stream);

        stream.Enqueue(SessionStarted(10));
        stream.OnWrite = request =>
        {
            if (request.RequestCase == SessionRequest.RequestOneofCase.AcquireSemaphore)
            {
                stream.Enqueue(AcquireResult(request.AcquireSemaphore.ReqId, acquired: true));
            }

            if (request.RequestCase == SessionRequest.RequestOneofCase.SessionStop)
            {
                stream.Enqueue(SessionStopped(10));
            }
        };

        await using var transport = new SessionTransport(
            driver.Object,
            NodePath,
            TestSessionOptions(),
            null);

        var acquired = await transport.AcquireSemaphore(
            "mutex",
            count: 1,
            isEphemeral: true,
            data: "owner"u8.ToArray(),
            timeout: TimeSpan.FromSeconds(3));

        Assert.True(acquired);

        var writes = stream.Writes.ToArray();
        var start = Assert.Single(writes.Where(request =>
            request.RequestCase == SessionRequest.RequestOneofCase.SessionStart));
        Assert.Equal(0UL, start.SessionStart.SessionId);
        Assert.Equal(NodePath, start.SessionStart.Path);
        Assert.NotEmpty(start.SessionStart.ProtectionKey);

        var acquire = Assert.Single(writes.Where(request =>
            request.RequestCase == SessionRequest.RequestOneofCase.AcquireSemaphore));
        Assert.Equal("mutex", acquire.AcquireSemaphore.Name);
        Assert.Equal(1UL, acquire.AcquireSemaphore.Count);
        Assert.True(acquire.AcquireSemaphore.Ephemeral);
        Assert.Equal((ulong)TimeSpan.FromSeconds(3).TotalMilliseconds, acquire.AcquireSemaphore.TimeoutMillis);
        Assert.Equal("owner"u8.ToArray(), acquire.AcquireSemaphore.Data.ToByteArray());
    }

    [Fact]
    public async Task SessionRecovery_WhenStreamCloses_ReusesSessionIdAndProtectionKey()
    {
        await using var firstStream = new TestCoordinationStream();
        await using var recoveredStream = new TestCoordinationStream();
        var driver = CreateDriver(firstStream, recoveredStream);

        firstStream.Enqueue(SessionStarted(42));
        firstStream.CompleteResponses();

        recoveredStream.Enqueue(SessionStarted(42));
        recoveredStream.OnWrite = request =>
        {
            if (request.RequestCase == SessionRequest.RequestOneofCase.AcquireSemaphore)
            {
                recoveredStream.Enqueue(AcquireResult(request.AcquireSemaphore.ReqId, acquired: true));
            }

            if (request.RequestCase == SessionRequest.RequestOneofCase.SessionStop)
            {
                recoveredStream.Enqueue(SessionStopped(42));
            }
        };

        await using var transport = new SessionTransport(
            driver.Object,
            NodePath,
            TestSessionOptions(),
            null);

        await WaitUntil(() => recoveredStream.Writes.Any(request =>
            request.RequestCase == SessionRequest.RequestOneofCase.SessionStart));

        Assert.True(await transport.AcquireSemaphore("lock", 1, true, null, TimeSpan.Zero));

        var initialStart = Assert.Single(firstStream.Writes.Where(request =>
            request.RequestCase == SessionRequest.RequestOneofCase.SessionStart));
        var recoveryStart = Assert.Single(recoveredStream.Writes.Where(request =>
            request.RequestCase == SessionRequest.RequestOneofCase.SessionStart));

        Assert.Equal(0UL, initialStart.SessionStart.SessionId);
        Assert.Equal(42UL, recoveryStart.SessionStart.SessionId);
        Assert.Equal(initialStart.SessionStart.ProtectionKey, recoveryStart.SessionStart.ProtectionKey);
        Assert.True(recoveryStart.SessionStart.SeqNo > initialStart.SessionStart.SeqNo);

        driver.Verify(d => d.BidirectionalStreamCall(
                It.IsAny<Method<SessionRequest, SessionResponse>>(),
                It.IsAny<GrpcRequestSettings>()),
            Times.Exactly(2));
    }

    [Fact]
    public async Task Ping_WhenReceived_RepliesWithPong()
    {
        await using var stream = new TestCoordinationStream();
        var driver = CreateDriver(stream);

        stream.Enqueue(SessionStarted(7));
        stream.Enqueue(new SessionResponse
        {
            Ping = new SessionResponse.Types.PingPong
            {
                Opaque = 12345
            }
        });

        stream.OnWrite = request =>
        {
            if (request.RequestCase == SessionRequest.RequestOneofCase.SessionStop)
            {
                stream.Enqueue(SessionStopped(7));
            }
        };

        await using var transport = new SessionTransport(
            driver.Object,
            NodePath,
            TestSessionOptions(),
            null);

        await WaitUntil(() => stream.Writes.Any(request =>
            request.RequestCase == SessionRequest.RequestOneofCase.Pong));

        var pong = Assert.Single(stream.Writes.Where(request =>
            request.RequestCase == SessionRequest.RequestOneofCase.Pong));
        Assert.Equal(12345UL, pong.Pong.Opaque);
    }

    [Fact]
    public async Task SessionRequestRegistry_ReconnectFaultsPendingRequestsAndAllowsNewRequests()
    {
        var registry = new SessionRequestRegistry();
        var reqId = registry.NextReqId();
        var pending = registry.Register(reqId, new SessionRequest
        {
            CreateSemaphore = new SessionRequest.Types.CreateSemaphore
            {
                Name = "semaphore",
                ReqId = reqId
            }
        });

        registry.Reconnect();

        await Assert.ThrowsAsync<YdbException>(async () => await pending.Tcs.Task);

        var nextReqId = registry.NextReqId();
        var nextPending = registry.Register(nextReqId, new SessionRequest
        {
            DeleteSemaphore = new SessionRequest.Types.DeleteSemaphore
            {
                Name = "semaphore",
                ReqId = nextReqId
            }
        });

        var response = new SessionResponse
        {
            DeleteSemaphoreResult = new SessionResponse.Types.DeleteSemaphoreResult
            {
                ReqId = nextReqId,
                Status = StatusIds.Types.StatusCode.Success
            }
        };

        Assert.True(registry.Resolve(nextReqId, response));
        Assert.Same(response, await nextPending.Tcs.Task);
    }

    [Fact]
    public async Task WatcherRegistry_RemapNotifyAndRemoveDeliversOnlyActiveSubscription()
    {
        using var registry = new WatcherRegistry();

        var oldSubscription = registry.Watch("semaphore");
        registry.RemapWatch("semaphore", oldSubscription, reqId: 1);

        var newSubscription = registry.Watch("semaphore");
        registry.RemapWatch("semaphore", newSubscription, reqId: 2);

        registry.Notify(new SessionResponse.Types.DescribeSemaphoreChanged
        {
            ReqId = 1
        });
        registry.Notify(new SessionResponse.Types.DescribeSemaphoreChanged
        {
            ReqId = 2
        });

        await AssertNoUpdate(oldSubscription);

        var received = await ReadNextUpdate(newSubscription);
        Assert.NotNull(received);

        registry.RemoveWatch("semaphore", newSubscription);
        registry.Notify(new SessionResponse.Types.DescribeSemaphoreChanged
        {
            ReqId = 2
        });

        await AssertNoUpdate(newSubscription);
    }

    private static SessionOptions TestSessionOptions() => new()
    {
        Description = "unit-test",
        StartTimeout = TimeSpan.FromSeconds(1),
        RecoveryWindow = TimeSpan.FromSeconds(1),
        RetryPolicy = new ImmediateRetryPolicy()
    };

    private static Mock<IDriver> CreateDriver(params TestCoordinationStream[] streams)
    {
        var queue = new Queue<TestCoordinationStream>(streams);
        var mock = new Mock<IDriver>();

        mock.Setup(driver => driver.BidirectionalStreamCall(
                It.IsAny<Method<SessionRequest, SessionResponse>>(),
                It.IsAny<GrpcRequestSettings>()))
            .Returns(() => new ValueTask<IBidirectionalStream<SessionRequest, SessionResponse>>(queue.Dequeue()));
        mock.Setup(driver => driver.LoggerFactory).Returns(Utils.LoggerFactory);

        return mock;
    }

    private static SessionResponse SessionStarted(ulong sessionId) => new()
    {
        SessionStarted = new SessionResponse.Types.SessionStarted
        {
            SessionId = sessionId
        }
    };

    private static SessionResponse SessionStopped(ulong sessionId) => new()
    {
        SessionStopped = new SessionResponse.Types.SessionStopped
        {
            SessionId = sessionId
        }
    };

    private static SessionResponse AcquireResult(ulong reqId, bool acquired) => new()
    {
        AcquireSemaphoreResult = new SessionResponse.Types.AcquireSemaphoreResult
        {
            ReqId = reqId,
            Status = StatusIds.Types.StatusCode.Success,
            Acquired = acquired
        }
    };

    private static async Task WaitUntil(Func<bool> predicate)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        while (!predicate())
        {
            await Task.Delay(10, cts.Token);
        }
    }

    private static async Task<SemaphoreChangedEvent> ReadNextUpdate(WatchSubscription subscription)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
        await using var enumerator = subscription.ReadAllAsync(cts.Token).GetAsyncEnumerator(cts.Token);

        Assert.True(await enumerator.MoveNextAsync());
        return enumerator.Current;
    }

    private static async Task AssertNoUpdate(WatchSubscription subscription)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await using var enumerator = subscription.ReadAllAsync(cts.Token).GetAsyncEnumerator(cts.Token);

        try
        {
            Assert.False(await enumerator.MoveNextAsync());
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
        }
    }

    private sealed class ImmediateRetryPolicy : IRetryPolicy
    {
        public TimeSpan? GetNextDelay(YdbException ydbException, int attempt) =>
            attempt < 10 ? TimeSpan.Zero : null;
    }

    private sealed class TestCoordinationStream : IBidirectionalStream<SessionRequest, SessionResponse>, IAsyncDisposable
    {
        private readonly Channel<SessionResponse> _responses = Channel.CreateUnbounded<SessionResponse>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });

        public ConcurrentQueue<SessionRequest> Writes { get; } = new();

        public Action<SessionRequest>? OnWrite { get; set; }

        public SessionResponse Current { get; private set; } = new();

        public Task Write(SessionRequest request)
        {
            Writes.Enqueue(request);
            OnWrite?.Invoke(request);
            return Task.CompletedTask;
        }

        public async Task<bool> MoveNextAsync()
        {
            if (!await _responses.Reader.WaitToReadAsync())
            {
                return false;
            }

            if (_responses.Reader.TryRead(out var response))
            {
                Current = response;
                return true;
            }

            return false;
        }

        public ValueTask<string?> AuthToken() => new((string?)null);

        public Task RequestStreamComplete()
        {
            CompleteResponses();
            return Task.CompletedTask;
        }

        public void Enqueue(SessionResponse response) => _responses.Writer.TryWrite(response);

        public void CompleteResponses() => _responses.Writer.TryComplete();

        public void Dispose() => CompleteResponses();

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
