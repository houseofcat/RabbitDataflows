﻿using HouseofCat.Dataflows.Pipelines;
using HouseofCat.RabbitMQ.Pipelines;
using HouseofCat.Serialization;
using HouseofCat.Utilities.Errors;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NETCore.MailKit;
using NETCore.MailKit.Core;
using NETCore.MailKit.Infrastructure.Internal;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace HouseofCat.RabbitMQ.Services
{
    public class EmailWorkerService : BackgroundService
    {
        private readonly ILogger<EmailWorkerService> _logger;
        private readonly IRabbitService _rabbitService;
        private readonly IConfiguration _config;
        private readonly EmailService _emailService;
        private readonly ISerializationProvider _serializationProvider;

        private Task RunningTask;

        public EmailWorkerService(
            IConfiguration config,
            ISerializationProvider serializationProvider,
            IRabbitService rabbitService,
            ILogger<EmailWorkerService> logger = null)
        {
            Guard.AgainstNull(config, nameof(config));
            Guard.AgainstNull(rabbitService, nameof(rabbitService));
            Guard.AgainstNull(serializationProvider, nameof(serializationProvider));

            _config = config;
            _serializationProvider = serializationProvider;
            _rabbitService = rabbitService;
            _logger = logger;

            var mailKitOptions = new MailKitOptions
            {
                // Use Papercut Smtp for local testing!
                // https://github.com/ChangemakerStudios/Papercut-SMTP/releases

                Server = _config.GetValue<string>("HouseofCat:EmailService:SmtpHost"),
                Port = _config.GetValue<int>("HouseofCat:EmailService:SmtpPort"),
                SenderName = _config.GetValue<string>("HouseofCat:EmailService:SenderName"),
                SenderEmail = _config.GetValue<string>("HouseofCat:EmailService:SenderEmail"),

                //Account = Configuration.GetValue<string>("Email:Account"),
                //Password = Configuration.GetValue<string>("Email:Password"),
                //Security = Configuration.GetValue<bool>("Email:EnableTls")
            };

            var provider = new MailKitProvider(mailKitOptions);
            _emailService = new EmailService(provider);
        }

        protected override async Task ExecuteAsync(CancellationToken token = default)
        {
            BuildEmailSendPipeline();
            RunningTask = SendEmailPipeline.StartAsync(false, token);

            while (!token.IsCancellationRequested)
            {
                if (RunningTask.IsCompleted && !RunningTask.IsFaulted)
                {
                    BuildEmailSendPipeline();
                    RunningTask = SendEmailPipeline.StartAsync(false, token);
                }
                else if (RunningTask.IsFaulted)
                {
                    if (RunningTask.Exception is not null)
                    {
                        var ex = RunningTask.Exception.Flatten().InnerException;
                        _logger.LogError("Error in excution.", ex);
                    }
                }

                await Task.Delay(1000, token).ConfigureAwait(false);
            }

            await _rabbitService.ShutdownAsync(false);
        }

        private ConsumerPipeline<SendEmailState> SendEmailPipeline { get; set; }
        private const string SendEmailConsumerName = "SendEmail.Consumer";
        private const string SendEmailFinalizeGenericError = "SendEmailFinalizeStep step failed to send an email. Nacking message...";

        private void BuildEmailSendPipeline()
        {
            var consumer = _rabbitService.GetConsumer(SendEmailConsumerName);
            var pipeline = new Pipeline<ReceivedData, SendEmailState>(consumer.ConsumerOptions.ConsumerPipelineOptions.MaxDegreesOfParallelism.Value);

            pipeline.AddStep<ReceivedData, SendEmailState>(SendEmailDeserialize);
            pipeline.AddAsyncStep<SendEmailState, SendEmailState>(SendEmailAsync);
            pipeline.Finalize((state) =>
            {
                if (state.StepSuccess[SendEmailStepKey])
                {
                    try
                    {
                        state.ReceivedData.AckMessage(); // Done sending an email.
                        state.ReceivedData.Complete(); // Tell the whole pipeline we are done with this instance.
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, SendEmailFinalizeGenericError);

                        state.ReceivedData.NackMessage(true); // Nack and requeue (for retry.)
                    }
                }
            });

            SendEmailPipeline = new ConsumerPipeline<SendEmailState>(
                _rabbitService.GetConsumer(consumer.ConsumerOptions.ConsumerName),
                pipeline);
        }

        public static readonly string SendEmailDeserializeStepKey = "SendEmailDeserializeStep";
        public static readonly string SendEmailDeserializeError = $"{nameof(SendEmailDeserialize)} step failed.";

        public SendEmailState SendEmailDeserialize(IReceivedData receivedData)
        {
            var state = new SendEmailState
            {
                ReceivedData = receivedData
            };

            try
            {
                state.SendEmail = state.ReceivedData.ContentType switch
                {
                    Constants.HeaderValueForLetter =>
                        _serializationProvider
                        .Deserialize<SendEmail>(state.ReceivedData.Letter.Body),

                    _ => _serializationProvider
                        .Deserialize<SendEmail>(state.ReceivedData.Data)
                };

                state.StepSuccess[SendEmailDeserializeStepKey] = state.SendEmail != default || state.SendEmail != null;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, SendEmailDeserializeError);
                state.StepSuccess[SendEmailDeserializeStepKey] = false;
            }

            return state;
        }

        public static readonly string SendEmailStepKey = "SendEmailStep";
        public static readonly string SendEmailError = $"{nameof(SendEmailAsync)} step failed.";

        public async Task<SendEmailState> SendEmailAsync(SendEmailState state)
        {
            if (state.StepSuccess[SendEmailDeserializeStepKey])
            {
                try
                {
                    await _emailService.SendAsync(
                        state.SendEmail.EmailAddress,
                        state.SendEmail.EmailSubject,
                        state.SendEmail.EmailBody,
                        state.SendEmail.IsHtml,
                        new SenderInfo()).ConfigureAwait(false);

                    state.StepSuccess[SendEmailStepKey] = true;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, SendEmailDeserializeError);
                    state.StepSuccess[SendEmailError] = false;
                }
            }

            return state;
        }
    }
}
