using HouseofCat.Dataflows.Pipelines;
using HouseofCat.RabbitMQ.Pipelines;
using HouseofCat.Serialization;
using HouseofCat.Utilities.Errors;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using Twilio;
using Twilio.Rest.Api.V2010.Account;

namespace HouseofCat.RabbitMQ.Services
{
    public class TextMessageWorkerService : BackgroundService
    {
        private readonly IConfiguration _config;
        private readonly string _consumerName;
        private readonly string _from;
        private readonly string _account;
        private readonly string _token;
        private readonly IRabbitService _rabbitService;
        private readonly ILogger<TextMessageWorkerService> _logger;
        private readonly ISerializationProvider _serializationProvider;
        private readonly ConsumerOptions _options;

        private IConsumerPipeline<TwilioWorkState> _consumerPipeline;
        private string _errorQueue;
        private Task RunningTask;

        public TextMessageWorkerService(
            IConfiguration config,
            IRabbitService rabbitService,
            ISerializationProvider serializationProvider,
            ILogger<TextMessageWorkerService> logger = null)
        {
            Guard.AgainstNull(config, nameof(config));
            Guard.AgainstNull(rabbitService, nameof(rabbitService));
            Guard.AgainstNull(rabbitService, nameof(rabbitService));
            Guard.AgainstNull(serializationProvider, nameof(serializationProvider));

            _logger = logger;
            _config = config;
            _rabbitService = rabbitService;
            _serializationProvider = serializationProvider;

            _consumerName = _config.GetSection("HouseofCat:NotificationService:ConsumerName").Get<string>();
            _options = _rabbitService.Options.GetConsumerOptions(_consumerName);

            _from = _config.GetSection("HouseofCat:NotificationService:From").Get<string>();
            _account = _config.GetSection("HouseofCat:NotificationService:Account").Get<string>();
            _token = _config.GetSection("HouseofCat:NotificationService:Token").Get<string>();

            TwilioClient.Init(_account, _token);
        }

        protected override async Task ExecuteAsync(CancellationToken token = default)
        {
            RunningTask = ProcessMessagesAsync(token);

            while (!token.IsCancellationRequested)
            {
                if (RunningTask.IsCompleted)
                {
                    RunningTask = ProcessMessagesAsync(token);
                }

                await Task.Delay(1000, token).ConfigureAwait(false);
            }

            await _rabbitService.ShutdownAsync(false);
        }

        private async Task<bool> SendMessageAsync(string message, string to, string from)
        {
            try
            {
                from = string.IsNullOrWhiteSpace(from) ? _from : from;
                if (!string.IsNullOrWhiteSpace(to)
                    && !string.IsNullOrWhiteSpace(message)
                    && !string.IsNullOrWhiteSpace(from))
                {
                    var response = await MessageResource.CreateAsync(
                        body: message,
                        from: new Twilio.Types.PhoneNumber(from),
                        to: new Twilio.Types.PhoneNumber(to)
                    ).ConfigureAwait(false);

                    if (response.Status == MessageResource.StatusEnum.Accepted)
                    { return true; }
                    else if (response.ErrorCode != 200 || response.ErrorCode != 201)
                    { _logger?.LogError("Twilio API Error: {0}", response.ErrorMessage); }
                }
                else
                {
                    _logger?.LogWarning("Details missing from text message. Skipped...");
                    return true;
                }
            }
            catch (Exception ex)
            {
                var stacky = ex.PrettifyStackTraceWithParameters(message, to, from);
                _logger?.LogError("Exception! Error: {0}\r\nStacktrace: {1}", ex.Message, stacky.ToJsonString());
            }

            return false;
        }

        // Processing Message Loop
        private async Task ProcessMessagesAsync(CancellationToken token = default)
        {
            try
            {
                _logger?.LogInformation($"Starting {nameof(TextMessageWorkerService)}...");

                _consumerPipeline = _rabbitService.CreateConsumerPipeline(_consumerName, BuildPipeline);
                _errorQueue = _options.ErrorQueueName;

                await _consumerPipeline.StartAsync(false, token).ConfigureAwait(false);
            }
            catch { /* SWALLOW */ }
        }

        // Build out your workflow
        public Pipeline<ReceivedData, TwilioWorkState> BuildPipeline(int maxDoP, bool? ensureOrdered)
        {
            var pipeline = new Pipeline<ReceivedData, TwilioWorkState>(
                maxDoP,
                healthCheckInterval: TimeSpan.FromSeconds(10),
                pipelineName: "Text.Message.Pipline");

            pipeline.AddStep<ReceivedData, TwilioWorkState>(DeserializeStep);
            pipeline.AddAsyncStep<TwilioWorkState, TwilioWorkState>(ProcessMessageStepAsync);
            pipeline.AddStep<TwilioWorkState, TwilioWorkState>(AckMessage);

            pipeline
                .Finalize(
                    (state) =>
                    {
                        if (state.AllStepsSuccess)
                        { _logger?.LogDebug($"{DateTime.Now:yyyy/MM/dd hh:mm:ss.fff} - Finished route successfully."); }
                        else
                        { _logger?.LogWarning($"{DateTime.Now:yyyy/MM/dd hh:mm:ss.fff} - Finished route unsuccesfully."); }

                        // Lastly mark the excution pipeline finished for this message.
                        state.ReceivedData?.Complete(); // This impacts wait to completion step in the WorkFlowEngine.
                    });

            return pipeline;
        }

        private TwilioWorkState DeserializeStep(IReceivedData receivedData)
        {
            var state = new TwilioWorkState
            {
                ReceivedData = receivedData
            };

            try
            {
                state.TextMessage = state.ReceivedData.ContentType switch
                {
                    Constants.HeaderValueForLetter =>
                        _serializationProvider
                        .Deserialize<TextMessage>(state.ReceivedData.Letter.Body),

                    _ => _serializationProvider
                        .Deserialize<TextMessage>(state.ReceivedData.Data)
                };

                if (state.ReceivedData.Data.Length > 0 && (state.TextMessage != null || state.ReceivedData.Letter != null))
                { state.DeserializeStepSuccess = true; }
            }
            catch
            { state.DeserializeStepSuccess = false; }

            return state;
        }

        private async Task<TwilioWorkState> ProcessMessageStepAsync(TwilioWorkState state)
        {
            if (state.DeserializeStepSuccess)
            {
                await SendMessageAsync(
                    state.TextMessage.Message,
                    state.TextMessage.ToNumber,
                    state.TextMessage.FromNumber).ConfigureAwait(false);

                state.ProcessStepSuccess = true;
            }
            else
            {
                // Park Failed Deserialize Steps
                var failed = await _rabbitService
                    .Publisher
                    .PublishAsync("", _errorQueue, state.ReceivedData.Data, null)
                    .ConfigureAwait(false);

                if (failed)
                { _logger?.LogError($"{DateTime.Now:yyyy/MM/dd hh:mm:ss.fff} - This failed to deserialize and publish to ErrorQueue!"); }
                else
                {
                    _logger?.LogError($"{DateTime.Now:yyyy/MM/dd hh:mm:ss.fff} - This failed to deserialize. Published to ErrorQueue ({_errorQueue})!");

                    // So we ack the message
                    state.ProcessStepSuccess = true;
                }
            }

            return state;
        }

        private TwilioWorkState AckMessage(TwilioWorkState state)
        {
            if (state.ProcessStepSuccess)
            {
                if (state.ReceivedData.AckMessage())
                { state.AcknowledgeStepSuccess = true; }
            }
            else
            {
                if (state.ReceivedData.NackMessage(true))
                { state.AcknowledgeStepSuccess = true; }
            }

            return state;
        }
    }
}
