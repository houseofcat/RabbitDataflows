﻿using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace RadAI.Metrics;

public class NullMetricsProvider : IMetricsProvider
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void DecrementCounter(string name, string unit = null, string description = null)
    { /* NO-OP */ }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void DecrementGauge(string name, string unit = null, string description = null)
    { /* NO-OP */ }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void IncrementCounter(string name, string unit = null, string description = null)
    { /* NO-OP */ }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void IncrementGauge(string name, string unit = null, string description = null)
    { /* NO-OP */ }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ObserveValue(string name, double value, string unit = null, string description = null)
    { /* NO-OP */ }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ObserveValueFluctuation(string name, double value, string unit = null, string description = null)
    { /* NO-OP */ }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Duration(string name, bool microScale = false, string unit = null, string description = null)
    {
        return null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Track(string name, string unit = null, string description = null)
    {
        return null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable TrackAndDuration(
        string name,
        bool microScale = false,
        string unit = null,
        string description = null,
        IDictionary<string, string> metricTags = null)
    {
        return null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Trace(
        string name,
        IDictionary<string, string> metricTags = null)
    {
        return null;
    }
}
