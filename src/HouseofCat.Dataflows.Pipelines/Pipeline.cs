﻿using HouseofCat.Logger;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace HouseofCat.Dataflows.Pipelines
{
    public interface IPipeline<TIn, TOut>
    {
        bool Ready { get; }
        int StepCount { get; }
        List<PipelineStep> Steps { get; }

        void AddAsyncStep<TLocalIn, TLocalOut>(Func<TLocalIn, Task<TLocalOut>> stepFunc, int? localMaxDoP = null, bool? ensureOrdered = null, int? bufferSizeOverride = null);
        void AddAsyncSteps<TLocalIn, TLocalOut>(List<Func<TLocalIn, Task<TLocalOut>>> stepFunctions, int? localMaxDoP = null, bool? ensureOrdered = null, int? bufferSizeOverride = null);
        void AddStep<TLocalIn, TLocalOut>(Func<TLocalIn, TLocalOut> stepFunc, int? localMaxDoP = null, bool? ensureOrdered = null, int? bufferSizeOverride = null);
        void AddSteps<TLocalIn, TLocalOut>(List<Func<TLocalIn, TLocalOut>> stepFunctions, int? localMaxDoP = null, bool? ensureOrdered = null, int? bufferSizeOverride = null);
        Task<bool> AwaitCompletionAsync();

        void Finalize(Action<TOut> finalizeStep);
        void Finalize(Func<TOut, Task> finalizeStep);
        Exception GetAnyPipelineStepsFault();
        Task<bool> QueueForExecutionAsync(TIn input);
    }

    // Great lesson/template found here.
    // https://michaelscodingspot.com/pipeline-implementations-csharp-3/

    public class Pipeline<TIn, TOut> : IPipeline<TIn, TOut>
    {
        private readonly ILogger<Pipeline<TIn, TOut>> _logger;
        private readonly ExecutionDataflowBlockOptions _executeStepOptions;
        private readonly DataflowLinkOptions _linkStepOptions;
        private readonly TimeSpan _healthCheckInterval;
        private readonly Task _healthCheckTask;
        private readonly string _pipelineName;
        private readonly CancellationTokenSource _cts;

        public List<PipelineStep> Steps { get; } = new List<PipelineStep>();
        public bool Ready { get; private set; }
        public int StepCount { get; private set; }

        public Pipeline(
            int maxDegreeOfParallelism,
            bool? ensureOrdered = null,
            int? bufferSize = null,
            TaskScheduler taskScheduler = null)
        {
            _cts = new CancellationTokenSource();
            _logger = LogHelper.GetLogger<Pipeline<TIn, TOut>>();

            _linkStepOptions = new DataflowLinkOptions { PropagateCompletion = true };
            _executeStepOptions = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = maxDegreeOfParallelism,
                SingleProducerConstrained = true
            };

            _executeStepOptions.EnsureOrdered = ensureOrdered ?? _executeStepOptions.EnsureOrdered;
            _executeStepOptions.BoundedCapacity = bufferSize ?? _executeStepOptions.BoundedCapacity;
            _executeStepOptions.TaskScheduler = taskScheduler ?? _executeStepOptions.TaskScheduler;
        }

        public Pipeline(
            int maxDegreeOfParallelism,
            TimeSpan healthCheckInterval,
            string pipelineName,
            bool? ensureOrdered = null,
            int? bufferSize = null,
            TaskScheduler taskScheduler = null) :
            this(maxDegreeOfParallelism, ensureOrdered, bufferSize, taskScheduler)
        {
            _pipelineName = pipelineName;
            _healthCheckInterval = healthCheckInterval;
            _healthCheckTask = Task.Run(SimplePipelineHealthTaskAsync);
        }

        public void AddAsyncStep<TLocalIn, TLocalOut>(
            Func<TLocalIn, Task<TLocalOut>> stepFunc,
            int? localMaxDoP = null,
            bool? ensureOrdered = null,
            int? bufferSizeOverride = null)
        {
            if (Ready) throw new InvalidOperationException(Constants.Pipelines.InvalidAddError);

            var options = GetExecuteStepOptions(localMaxDoP, ensureOrdered, bufferSizeOverride);
            var pipelineStep = new PipelineStep
            {
                IsAsync = true,
                StepIndex = StepCount++,
            };

            if (Steps.Count == 0)
            {
                pipelineStep.Block = new TransformBlock<TLocalIn, Task<TLocalOut>>(stepFunc, options);
                Steps.Add(pipelineStep);
            }
            else
            {
                var lastStep = Steps.Last();
                if (lastStep.IsAsync)
                {
                    var step = new TransformBlock<Task<TLocalIn>, Task<TLocalOut>>(
                        async (input) => stepFunc(await input.ConfigureAwait(false)),
                        options);

                    if (lastStep.Block is ISourceBlock<Task<TLocalIn>> sourceBlock)
                    {
                        sourceBlock.LinkTo(step, _linkStepOptions);
                        pipelineStep.Block = step;
                        Steps.Add(pipelineStep);
                    }
                    else { throw new InvalidOperationException(Constants.Pipelines.InvalidStepFound); }
                }
                else
                {
                    var step = new TransformBlock<TLocalIn, Task<TLocalOut>>(stepFunc, options);

                    if (lastStep.Block is ISourceBlock<TLocalIn> sourceBlock)
                    {
                        sourceBlock.LinkTo(step, _linkStepOptions);
                        pipelineStep.Block = step;
                        Steps.Add(pipelineStep);
                    }
                    else { throw new InvalidOperationException(Constants.Pipelines.InvalidStepFound); }
                }
            }
        }

        public void AddAsyncSteps<TLocalIn, TLocalOut>(
            List<Func<TLocalIn, Task<TLocalOut>>> stepFunctions,
            int? localMaxDoP = null,
            bool? ensureOrdered = null,
            int? bufferSizeOverride = null)
        {
            if (Ready) throw new InvalidOperationException(Constants.Pipelines.InvalidAddError);

            for (int i = 0; i < stepFunctions.Count; i++)
            {
                AddAsyncStep(stepFunctions[i], localMaxDoP, ensureOrdered, bufferSizeOverride);
            }
        }

        public void AddStep<TLocalIn, TLocalOut>(
            Func<TLocalIn, TLocalOut> stepFunc,
            int? localMaxDoP = null,
            bool? ensureOrdered = null,
            int? bufferSizeOverride = null)
        {
            if (Ready) throw new InvalidOperationException(Constants.Pipelines.InvalidAddError);

            var options = GetExecuteStepOptions(localMaxDoP, ensureOrdered, bufferSizeOverride);
            var pipelineStep = new PipelineStep
            {
                IsAsync = false,
                StepIndex = StepCount++,
            };

            if (Steps.Count == 0)
            {
                pipelineStep.Block = new TransformBlock<TLocalIn, TLocalOut>(stepFunc, options);
                Steps.Add(pipelineStep);
            }
            else
            {
                var lastStep = Steps.Last();
                if (lastStep.IsAsync)
                {
                    var step = new TransformBlock<Task<TLocalIn>, TLocalOut>(
                        async (input) => stepFunc(await input.ConfigureAwait(false)),
                        options);

                    if (lastStep.Block is ISourceBlock<Task<TLocalIn>> sourceBlock)
                    {
                        sourceBlock.LinkTo(step, _linkStepOptions);
                        pipelineStep.Block = step;
                        Steps.Add(pipelineStep);
                    }
                    else { throw new InvalidOperationException(Constants.Pipelines.InvalidStepFound); }
                }
                else
                {
                    var step = new TransformBlock<TLocalIn, TLocalOut>(stepFunc, options);
                    if (lastStep.Block is ISourceBlock<TLocalIn> sourceBlock)
                    {
                        sourceBlock.LinkTo(step, _linkStepOptions);
                        pipelineStep.Block = step;
                        Steps.Add(pipelineStep);
                    }
                    else { throw new InvalidOperationException(Constants.Pipelines.InvalidStepFound); }
                }
            }
        }

        public void AddSteps<TLocalIn, TLocalOut>(
            List<Func<TLocalIn, TLocalOut>> stepFunctions,
            int? localMaxDoP = null,
            bool? ensureOrdered = null,
            int? bufferSizeOverride = null)
        {
            if (Ready) throw new InvalidOperationException(Constants.Pipelines.InvalidAddError);

            for (int i = 0; i < stepFunctions.Count; i++)
            {
                AddStep(stepFunctions[i], localMaxDoP, ensureOrdered, bufferSizeOverride);
            }
        }

        public void Finalize(Action<TOut> finalizeStep)
        {
            if (Ready) throw new InvalidOperationException(Constants.Pipelines.AlreadyFinalized);
            if (Steps.Count == 0) throw new InvalidOperationException(Constants.Pipelines.CantFinalize);

            if (finalizeStep != null)
            {
                var pipelineStep = new PipelineStep
                {
                    IsAsync = false,
                    StepIndex = StepCount++,
                    IsLastStep = true,
                };

                var lastStep = Steps.Last();
                if (lastStep.IsAsync)
                {
                    var step = new ActionBlock<Task<TOut>>(
                        async input => finalizeStep(await input.ConfigureAwait(false)),
                        _executeStepOptions);

                    if (lastStep.Block is ISourceBlock<Task<TOut>> sourceBlock)
                    {
                        pipelineStep.Block = step;
                        sourceBlock.LinkTo(step, _linkStepOptions);
                        Steps.Add(pipelineStep);
                    }
                    else { throw new InvalidOperationException(Constants.Pipelines.InvalidStepFound); }
                }
                else
                {
                    var step = new ActionBlock<TOut>(
                        finalizeStep,
                        _executeStepOptions);

                    if (lastStep.Block is ISourceBlock<TOut> sourceBlock)
                    {
                        pipelineStep.Block = step;
                        sourceBlock.LinkTo(step, _linkStepOptions);
                        Steps.Add(pipelineStep);
                    }
                    else { throw new InvalidOperationException(Constants.Pipelines.InvalidStepFound); }
                }
            }
            else
            { Steps.Last().IsLastStep = true; }

            Ready = true;
        }

        public void Finalize(Func<TOut, Task> finalizeStep)
        {
            if (Ready) throw new InvalidOperationException(Constants.Pipelines.AlreadyFinalized);
            if (Steps.Count == 0) throw new InvalidOperationException(Constants.Pipelines.CantFinalize);

            if (finalizeStep != null)
            {
                var pipelineStep = new PipelineStep
                {
                    IsAsync = true,
                    StepIndex = StepCount++,
                    IsLastStep = true,
                };

                var lastStep = Steps.Last();
                if (lastStep.IsAsync)
                {
                    var step = new ActionBlock<Task<TOut>>(
                        async t => await finalizeStep(await t.ConfigureAwait(false)).ConfigureAwait(false),
                        _executeStepOptions);

                    if (lastStep.Block is ISourceBlock<Task<TOut>> sourceBlock)
                    {
                        pipelineStep.Block = step;
                        sourceBlock.LinkTo(step, _linkStepOptions);
                        Steps.Add(pipelineStep);
                    }
                }
                else
                {
                    var step = new ActionBlock<TOut>(t => finalizeStep(t), _executeStepOptions);
                    if (lastStep.Block is ISourceBlock<TOut> sourceBlock)
                    {
                        pipelineStep.Block = step;
                        sourceBlock.LinkTo(step, _linkStepOptions);
                        Steps.Add(pipelineStep);
                    }
                }
            }
            else
            {
                var lastStep = Steps.Last();
                lastStep.IsLastStep = true;
            }

            Ready = true;
        }

        public async Task<bool> QueueForExecutionAsync(TIn input)
        {
            if (!Ready) throw new InvalidOperationException(Constants.Pipelines.NotFinalized);

            if (Steps[0].Block is ITargetBlock<TIn> firstStep)
            {
                _logger.LogTrace(Constants.Pipelines.Queued, _pipelineName);

                return await firstStep
                    .SendAsync(input)
                    .ConfigureAwait(false);
            }

            return false;
        }

        public async Task<bool> AwaitCompletionAsync()
        {
            if (!Ready) throw new InvalidOperationException(Constants.Pipelines.NotFinalized);

            if (Steps[0].Block is ITargetBlock<TIn> firstStep)
            {
                // Tell the pipeline its finished.
                firstStep.Complete();

                // Await the last step.
                if (Steps[^1].Block is ITargetBlock<TIn> lastStep)
                {
                    _logger.LogTrace(Constants.Pipelines.AwaitsCompletion, _pipelineName);
                    await lastStep.Completion.ConfigureAwait(false);
                    return true;
                }
            }

            _cts?.Cancel();

            return false;
        }

        public Exception GetAnyPipelineStepsFault()
        {
            foreach (var step in Steps)
            {
                if (step.IsFaulted)
                {
                    return step.Block.Completion.Exception;
                }
            }

            return null;
        }

        private ExecutionDataflowBlockOptions GetExecuteStepOptions(int? maxDoPOverride, bool? ensureOrdered, int? bufferSizeOverride)
        {
            if (maxDoPOverride.HasValue || ensureOrdered.HasValue || bufferSizeOverride.HasValue)
            {
                return new ExecutionDataflowBlockOptions
                {
                    EnsureOrdered = ensureOrdered ?? _executeStepOptions.EnsureOrdered,
                    MaxDegreeOfParallelism = maxDoPOverride ?? _executeStepOptions.MaxDegreeOfParallelism,
                    BoundedCapacity = bufferSizeOverride ?? _executeStepOptions.BoundedCapacity,
                    TaskScheduler = _executeStepOptions.TaskScheduler
                };
            }

            return _executeStepOptions;
        }

        private async Task SimplePipelineHealthTaskAsync()
        {
            await Task.Yield();

            while (!_cts.IsCancellationRequested)
            {
                try
                { await Task.Delay(_healthCheckInterval, _cts.Token).ConfigureAwait(false); }
                catch { /* SWALLOW */ }

                var ex = GetAnyPipelineStepsFault();
                if (ex != null)
                { _logger.LogCritical(ex, Constants.Pipelines.Faulted, _pipelineName); }
                else  // No Steps are Faulted... Hooray!
                { _logger.LogInformation(Constants.Pipelines.Healthy, _pipelineName); }
            }
        }
    }
}
