using HouseofCat.Logger;
using HouseofCat.Utilities.Errors;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace HouseofCat.RabbitMQ.Pools;

public interface IChannelPool
{
    RabbitOptions Options { get; }
    ulong CurrentChannelId { get; }
    bool Shutdown { get; }

    /// <summary>
    /// This pulls an ackable <see cref="IChannelHost"/> out of the <see cref="IChannelPool"/> for usage.
    /// <para>If the <see cref="IChannelHost"/> was previously flagged on error, multi-attempts to recreate it before returning an open channel back to the user.
    /// If you only remove channels and never add them, you will drain your <see cref="IChannelPool"/>.</para>
    /// <para>Use <see cref="ReturnChannelAsync"/> to return Channels.</para>
    /// <para><em>Note: During an outage event, you will pause here until a viable channel can be acquired.</em></para>
    /// </summary>
    /// <returns><see cref="IChannelHost"/></returns>
    ValueTask<IChannelHost> GetAckChannelAsync();

    /// <summary>
    /// This pulls a <see cref="IChannelHost"/> out of the <see cref="IChannelPool"/> for usage.
    /// <para>If the <see cref="IChannelHost"/> was previously flagged on error, multi-attempts to recreate it before returning an open channel back to the user.
    /// If you only remove channels and never add them, you will drain your <see cref="IChannelPool"/>.</para>
    /// <para>Use <see cref="ReturnChannelAsync"/> to return the <see cref="IChannelHost"/>.</para>
    /// <para><em>Note: During an outage event, you will pause here until a viable channel can be acquired.</em></para>
    /// </summary>
    /// <returns><see cref="IChannelHost"/></returns>
    ValueTask<IChannelHost> GetChannelAsync();

    /// <summary>
    /// <para>Gives user a transient <see cref="IChannelHost"/> is simply a channel not managed by this library.</para>
    /// <para><em>Closing and disposing the <see cref="IChannelHost"/> is the responsiblity of the user.</em></para>
    /// </summary>
    /// <param name="ackable"></param>
    /// <returns><see cref="IChannelHost"/></returns>
    ValueTask<IChannelHost> GetTransientChannelAsync(bool ackable);

    ValueTask ReturnChannelAsync(IChannelHost chanHost, bool flagChannel = false);
    Task ShutdownAsync();
}

public class ChannelPool : IChannelPool, IDisposable
{
    private readonly ILogger<ChannelPool> _logger;
    private readonly IConnectionPool _connectionPool;
    private readonly SemaphoreSlim _poolLock = new SemaphoreSlim(1, 1);
    private Channel<IChannelHost> _channels;
    private Channel<IChannelHost> _ackChannels;
    private ConcurrentDictionary<ulong, bool> _flaggedChannels;
    private bool _disposedValue;

    public RabbitOptions Options { get; }

    // A 0 indicates TransientChannels.
    public ulong CurrentChannelId { get; private set; } = 1;
    public bool Shutdown { get; private set; }

    private readonly CancellationTokenSource _cts;

    public ChannelPool(RabbitOptions options) : this(new ConnectionPool(options)) { }

    public ChannelPool(IConnectionPool connPool)
    {
        Guard.AgainstNull(connPool, nameof(connPool));
        Options = connPool.Options;

        _currentTansientChannelId =
            Options.PoolOptions.TansientChannelStartRange < 10000
            ? 10000
            : Options.PoolOptions.TansientChannelStartRange;

        _logger = LogHelper.GetLogger<ChannelPool>();
        _connectionPool = connPool;
        _flaggedChannels = new ConcurrentDictionary<ulong, bool>();

        if (Options.PoolOptions.MaxChannels > 0)
        {
            _channels = Channel.CreateBounded<IChannelHost>(Options.PoolOptions.MaxChannels);
        }

        if (Options.PoolOptions.MaxAckableChannels > 0)
        {
            _ackChannels = Channel.CreateBounded<IChannelHost>(Options.PoolOptions.MaxAckableChannels);
        }

        if (!Options.PoolOptions.OnlyTransientChannels)
        {
            CreateChannelsAsync().GetAwaiter().GetResult();
        }

        _cts = new CancellationTokenSource();
    }

    private async Task CreateChannelsAsync()
    {
        for (var i = 0; i < Options.PoolOptions.MaxChannels; i++)
        {
            var chanHost = await CreateChannelAsync(CurrentChannelId++, false).ConfigureAwait(false);

            await _channels
                .Writer
                .WriteAsync(chanHost);
        }

        for (var i = 0; i < Options.PoolOptions.MaxAckableChannels; i++)
        {
            var chanHost = await CreateChannelAsync(CurrentChannelId++, true).ConfigureAwait(false);

            await _ackChannels
                .Writer
                .WriteAsync(chanHost);
        }
    }

    /// <summary>
    /// This pulls a <see cref="IChannelHost"/> out of the <see cref="IChannelPool"/> for usage.
    /// <para>If the <see cref="IChannelHost"/> was previously flagged on error, multi-attempta to recreate it before returning an open channel back to the user.
    /// If you only remove channels and never add them, you will drain your <see cref="IChannelPool"/>.</para>
    /// <para>Use <see cref="ReturnChannelAsync"/> to return the <see cref="IChannelHost"/>.</para>
    /// <para><em>Note: During an outage event, you will pause here until a viable channel can be acquired.</em></para>
    /// </summary>
    /// <returns><see cref="IChannelHost"/></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async ValueTask<IChannelHost> GetChannelAsync()
    {
        if (Shutdown) throw new InvalidOperationException(ExceptionMessages.ChannelPoolValidationMessage);

        if (Options.PoolOptions.OnlyTransientChannels)
        {
            return await GetTransientChannelAsync(false)
                .ConfigureAwait(false);
        }

        if (_channels == null)
        {
            throw new InvalidOperationException(ExceptionMessages.ChannelPoolBadOptionMaxChannelError);
        }

        if (!await _channels
            .Reader
            .WaitToReadAsync()
            .ConfigureAwait(false))
        {
            throw new InvalidOperationException(ExceptionMessages.ChannelPoolGetChannelError);
        }

        var chanHost = await _channels
            .Reader
            .ReadAsync()
            .ConfigureAwait(false);

        var healthy = await chanHost.ChannelHealthyAsync().ConfigureAwait(false);
        var flagged = _flaggedChannels.ContainsKey(chanHost.ChannelId) && _flaggedChannels[chanHost.ChannelId];
        if (flagged || !healthy)
        {
            _logger.LogWarning(LogMessages.ChannelPools.ChannelHasIssues, chanHost.ChannelId);

            await chanHost.WaitUntilChannelIsReadyAsync(Options.PoolOptions.SleepOnErrorInterval, _cts.Token);
        }
        else if (healthy && chanHost.FlowControlled)
        {
            while (chanHost.FlowControlled)
            {
                try
                {
                    await Task
                        .Delay(Options.PoolOptions.SleepOnErrorInterval, _cts.Token)
                        .ConfigureAwait(false);
                }
                catch { /* SWALLOW */ }
            }
        }

        return chanHost;
    }

    /// <summary>
    /// This pulls an ackable <see cref="IChannelHost"/> out of the <see cref="IChannelPool"/> for usage.
    /// <para>If the <see cref="IChannelHost"/> was previously flagged on error, multi-attempta to recreate it before returning an open channel back to the user.
    /// If you only remove channels and never add them, you will drain your <see cref="IChannelPool"/>.</para>
    /// <para>Use <see cref="ReturnChannelAsync"/> to return Channels.</para>
    /// <para><em>Note: During an outage event, you will pause here until a viable channel can be acquired.</em></para>
    /// </summary>
    /// <returns><see cref="IChannelHost"/></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async ValueTask<IChannelHost> GetAckChannelAsync()
    {
        if (Shutdown) throw new InvalidOperationException(ExceptionMessages.ChannelPoolValidationMessage);

        if (Options.PoolOptions.OnlyTransientChannels)
        {
            return await GetTransientChannelAsync(true)
                .ConfigureAwait(false);
        }

        if (_ackChannels == null)
        {
            throw new InvalidOperationException(ExceptionMessages.ChannelPoolBadOptionMaxAckChannelError);
        }

        if (!await _ackChannels
            .Reader
            .WaitToReadAsync()
            .ConfigureAwait(false))
        {
            throw new InvalidOperationException(ExceptionMessages.ChannelPoolGetChannelError);
        }

        var chanHost = await _ackChannels
            .Reader
            .ReadAsync()
            .ConfigureAwait(false);

        var healthy = await chanHost.ChannelHealthyAsync().ConfigureAwait(false);
        var flagged = _flaggedChannels.ContainsKey(chanHost.ChannelId) && _flaggedChannels[chanHost.ChannelId];
        if (flagged || !healthy)
        {
            _logger.LogWarning(LogMessages.ChannelPools.ChannelHasIssues, chanHost.ChannelId);

            await chanHost.WaitUntilChannelIsReadyAsync(Options.PoolOptions.SleepOnErrorInterval, _cts.Token);
        }
        else if (healthy && chanHost.FlowControlled)
        {
            while (chanHost.FlowControlled)
            {
                try
                {
                    await Task
                        .Delay(100, _cts.Token)
                        .ConfigureAwait(false);
                }
                catch { /* SWALLOW */ }
            }
        }

        return chanHost;
    }

    // This is a simple counter to give a unique id to transient channels.
    private ulong _currentTansientChannelId = 10000;

    private ulong GetNextTransientChannelId() => Interlocked.Increment(ref _currentTansientChannelId);

    /// <summary>
    /// <para>Gives user a transient <see cref="IChannelHost"/> is simply a channel not managed by this library.</para>
    /// <para><em>Closing and disposing the <see cref="IChannelHost"/> is the responsiblity of the user.</em></para>
    /// </summary>
    /// <param name="ackable"></param>
    /// <returns><see cref="IChannelHost"/></returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async ValueTask<IChannelHost> GetTransientChannelAsync(bool ackable) => await CreateChannelAsync(GetNextTransientChannelId(), ackable).ConfigureAwait(false);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private async Task<IChannelHost> CreateChannelAsync(ulong channelId, bool ackable)
    {
        IConnectionHost connHost = null;

        while (true)
        {
            _logger.LogTrace(LogMessages.ChannelPools.CreateChannel, channelId);

            // Get ConnectionHost
            try
            { connHost = await _connectionPool.GetConnectionAsync().ConfigureAwait(false); }
            catch
            {
                _logger.LogTrace(LogMessages.ChannelPools.CreateChannelFailedConnection, channelId);
                await ReturnConnectionWithOptionalSleep(connHost, channelId, Options.PoolOptions.SleepOnErrorInterval).ConfigureAwait(false);
                continue;
            }

            // Create a Channel Host
            try
            {
                var chanHost = new ChannelHost(channelId, connHost, ackable);
                await ReturnConnectionWithOptionalSleep(connHost, channelId, 0).ConfigureAwait(false);
                _flaggedChannels[chanHost.ChannelId] = false;
                _logger.LogDebug(LogMessages.ChannelPools.CreateChannelSuccess, channelId);

                return chanHost;
            }
            catch
            {
                _logger.LogTrace(LogMessages.ChannelPools.CreateChannelFailedConstruction, channelId);
                await ReturnConnectionWithOptionalSleep(connHost, channelId, Options.PoolOptions.SleepOnErrorInterval).ConfigureAwait(false);
            }
        }
    }

    private async Task ReturnConnectionWithOptionalSleep(IConnectionHost connHost, ulong channelId, int sleep)
    {
        if (connHost != null)
        { await _connectionPool.ReturnConnectionAsync(connHost); } // Return Connection (or lose them.)

        if (sleep > 0)
        {
            _logger.LogDebug(LogMessages.ChannelPools.CreateChannelSleep, channelId);

            await Task
                .Delay(sleep)
                .ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Returns the <see cref="ChannelHost"/> back to the <see cref="ChannelPool"/>.
    /// <para>All Aqmp IModel Channels close server side on error, so you have to indicate to the library when that happens.</para>
    /// <para>The library does its best to listen for a dead <see cref="ChannelHost"/>, but nothing is as reliable as the user flagging the channel for replacement.</para>
    /// <para><em>Users flag the channel for replacement (e.g. when an error occurs) on it's next use.</em></para>
    /// </summary>
    /// <param name="chanHost"></param>
    /// <param name="flagChannel"></param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public async ValueTask ReturnChannelAsync(IChannelHost chanHost, bool flagChannel = false)
    {
        if (Shutdown) throw new InvalidOperationException(ExceptionMessages.ChannelPoolValidationMessage);

        _flaggedChannels[chanHost.ChannelId] = flagChannel;

        _logger.LogDebug(LogMessages.ChannelPools.ReturningChannel, chanHost.ChannelId, flagChannel);

        if (chanHost.Ackable)
        {
            await _ackChannels
                .Writer
                .WriteAsync(chanHost)
                .ConfigureAwait(false);
        }
        else
        {
            await _channels
                .Writer
                .WriteAsync(chanHost)
                .ConfigureAwait(false);
        }
    }

    public async Task ShutdownAsync()
    {
        _logger.LogTrace(LogMessages.ChannelPools.Shutdown);

        await _poolLock
            .WaitAsync()
            .ConfigureAwait(false);

        if (!Shutdown)
        {
            _cts.Cancel();

            await CloseChannelsAsync()
                .ConfigureAwait(false);

            Shutdown = true;

            await _connectionPool
                .ShutdownAsync()
                .ConfigureAwait(false);
        }

        _poolLock.Release();
        _logger.LogTrace(LogMessages.ChannelPools.ShutdownComplete);
    }

    private async Task CloseChannelsAsync()
    {
        // Signal to Channel no more data is coming.
        _channels?.Writer.Complete();
        _ackChannels?.Writer.Complete();

        if (_channels is not null)
        {
            await _channels.Reader.WaitToReadAsync().ConfigureAwait(false);
            while (_channels.Reader.TryRead(out IChannelHost chanHost))
            {
                try
                { chanHost.Close(); }
                catch { /* SWALLOW */ }
            }
        }

        if (_ackChannels is not null)
        {
            await _ackChannels.Reader.WaitToReadAsync().ConfigureAwait(false);
            while (_ackChannels.Reader.TryRead(out IChannelHost chanHost))
            {
                try
                { chanHost.Close(); }
                catch { /* SWALLOW */ }
            }
        }
    }

    protected virtual void Dispose(bool disposing)
    {
        if (!_disposedValue)
        {
            if (disposing)
            {
                _channels = null;
                _ackChannels = null;
                _flaggedChannels = null;
                _poolLock.Dispose();
            }

            _disposedValue = true;
        }
    }

    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}
