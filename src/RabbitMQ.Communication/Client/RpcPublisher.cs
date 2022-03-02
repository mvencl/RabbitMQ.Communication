using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Communication.Context;
using RabbitMQ.Communication.Contracts;
using RabbitMQ.Communication.Extension;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Communication.Client
{
    public class RpcPublisher : IDisposable
    {
        //https://github.com/rabbitmq/rabbitmq-dotnet-client/issues/874

        /// <summary>
        /// Maximum timeout in milliseconds
        /// - set on 3hours (3*3600*1000) = 10 800 000
        /// </summary>
        private const int MAX_TIMEOUT_MS = 10800000;

        #region Dispose
        private bool disposedValue;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                Channel.BasicReturn -= Channel_BasicReturn;
                if (disposing)
                {
                    Publisher.Dispose();
                    Subscriber.Dispose();
                    CancellationTokenSource?.Dispose();

                    // TODO: dispose managed state (managed objects)
                    if (DisposeChannel) Channel.Dispose();
                }
                // TODO: free unmanaged resources (unmanaged objects) and override finalizer                
                foreach (TaskCompletionSource<BaseResponseMessageContext> t in callbackMapper.Values)
                {
                    t.TrySetCanceled();
                }
                callbackMapper.Clear();

                // TODO: set large fields to null                

                disposedValue = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        // ~RpcPublisher()
        // {
        //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        //     Dispose(disposing: false);
        // }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
        #endregion Dispose

        internal Subscriber<BaseResponseMessageContext> Subscriber { get; }
        internal Publisher Publisher { get; }
        internal IModel Channel { get; }
        internal ILogger Logger { get; }
        private bool DisposeChannel { get; } = false;

        private readonly ConcurrentDictionary<string, TaskCompletionSource<BaseResponseMessageContext>> callbackMapper =
                new ConcurrentDictionary<string, TaskCompletionSource<BaseResponseMessageContext>>();

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connection">Connection</param>
        public RpcPublisher(IConnection connection, string subscriberExchangeName = "amq.direct", ILogger logger = null) : this(connection.CreateModel(), subscriberExchangeName, logger)
        {
            // Channel is created in this class please dispose this channel
            DisposeChannel = true;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="channel">Channel</param>
        public RpcPublisher(IModel channel, string subscriberExchangeName = "amq.direct", ILogger logger = null)
        {
            Logger = logger;
            Channel = channel;
            Channel.BasicReturn += Channel_BasicReturn;
            Publisher = new Publisher(channel);
            Subscriber = new Subscriber<BaseResponseMessageContext>(channel, RabbitMQExtension.GetDefaultSubscriberRoutingKey, ConsumerFunctionAsync, subscriberExchangeName ?? RabbitMQExtension.GetDefaultSubscriberExchangeName, allowCancellation: false, logger: logger);
        }

        private async Task ConsumerFunctionAsync(BaseResponseMessageContext message, BasicDeliverEventArgs ea, CancellationToken ct = default)
        {
            try
            {
                if (callbackMapper.TryRemove(ea.BasicProperties.CorrelationId, out TaskCompletionSource<BaseResponseMessageContext> tcs))
                {
                    if (message.IsError)
                    {
                        tcs.SetException(message.Exception);
                        Logger?.LogError(message.Exception, "RpcPublisher.ConsumerFunction Message with exception was received. correlationId: {correlationId}", ea.BasicProperties.CorrelationId);
                    }
                    else
                    {
                        tcs.SetResult(message);
                    }

                    await tcs.Task;
                }
                else
                {
                    Logger?.LogWarning("RpcPublisher.ConsumerFunction correlationID: {correlationId} was not found. It was for {Exchange} -> {RoutingKey}", ea.BasicProperties.CorrelationId, ea.Exchange, ea.RoutingKey);
                    Logger?.LogDebug("CorrelationId:{correlationId} and Context:{Context}", ea.BasicProperties.CorrelationId, message.Context);
                }
            }
            catch (Exception ex)
            {
                Logger?.LogError(ex, "RpcPublisher.ConsumerFunction ended with error for correlationId:{correlationId}", ea.BasicProperties.CorrelationId);
                if (ex.GetType() != message.Exception.GetType())
                    throw ex;
            }

        }

        private CancellationTokenSource CancellationTokenSource { get; set; } = null;

        /// <summary>
        /// Function for sending a message for which we are not waiting for a reply.
        /// </summary>
        /// <param name="requestRoutingKey">Routing key to use for sending the message. For separation, it must contain dots no slashes.</param>
        /// <param name="message">Message to be sent.</param>
        /// <param name="publisherExchangeName">If not set, the value from the PublisherExchangeNameConfig property is used.</param>
        public async Task<BaseResponseMessageContext> SendAsync(string routingKey, IMessageContext message, int timeoutSec = 0, string exchangeName = "amq.topic", CancellationToken ct = default)
        {
            if (message == null)
                throw new ArgumentNullException(nameof(message));

            string correlationId = RabbitMQExtension.GetCorrelationId();

            if (callbackMapper.ContainsKey(correlationId))
                throw new DuplicateWaitObjectException(nameof(correlationId), $"A message with this correlationId: {correlationId} has already been used. Please generate new ones.");

            try
            {
                TaskCompletionSource<BaseResponseMessageContext> tcs = new TaskCompletionSource<BaseResponseMessageContext>(TaskCreationOptions.RunContinuationsAsynchronously);
                if (!callbackMapper.TryAdd(correlationId, tcs))
                    throw new Exception($"CorrelationId: {correlationId} was not added");

                //setting max timeout
                CancellationTokenSource = new CancellationTokenSource(TimeoutMs(timeoutSec));
                CancellationTokenRegistration timeoutToken = CancellationTokenSource.Token.Register(
                    async () =>
                    {
                        Logger?.LogWarning("RpcPublisher.SendAsync Request was canceled by timeout. CorrelationId:{correlationId}", correlationId);
                        if (callbackMapper.TryRemove(correlationId, out var tmp))
                            tmp.SetCanceled();
                        await Publisher.SendAsync(correlationId, new BaseMessageContext() { Context = "Canceled by timeout" }, exchangeName: "amq.fanout", correlationId: correlationId);
                    });
                CancellationTokenRegistration userToken = ct.Register(
                    async () =>
                    {
                        Logger?.LogWarning("RpcPublisher.SendAsync Request was canceled by user. CorrelationId:{correlationId}", correlationId);
                        if (callbackMapper.TryRemove(correlationId, out var tmp1))
                            tmp1.SetCanceled();
                        await Publisher.SendAsync(correlationId, new BaseMessageContext() { Context = "Canceled by user" }, exchangeName: "amq.fanout", correlationId: correlationId);
                    });

                Publisher.ReplyTo replyTo = new Publisher.ReplyTo()
                {
                    ExchangeName = Subscriber.ExchangeName,
                    RoutingKey = Subscriber.RoutingKey
                };

                Logger?.LogDebug("A message with correlationId: {correlationId} will be send. Please reply to {ExchangeName} -> {RoutingKey}", correlationId, replyTo.ExchangeName, replyTo.RoutingKey);

                await Publisher.SendAsync(routingKey, message, exchangeName, correlationId: correlationId, replyTo: replyTo, ct: ct, mandatory: true);

                var test = await tcs.Task;

                timeoutToken.Dispose();
                userToken.Dispose();

                return test;

            }
            catch (Exception ex)
            {
                Logger?.LogError(ex, "RpcPublisher.SendAsync catch exception with correlationId:{correlationId}", correlationId);
                if (callbackMapper.TryRemove(correlationId, out var tmp))
                    tmp.SetException(ex);
                throw ex;
            }

        }

        private void Channel_BasicReturn(object sender, BasicReturnEventArgs e)
        {
            if (e.ReplyCode == 312)
            {
                callbackMapper.TryRemove(e.BasicProperties.CorrelationId, out TaskCompletionSource<BaseResponseMessageContext> tcsMess);
                tcsMess?.SetException(new MissingMethodException($"Route not found. ReplyCodeCode: {e.ReplyCode}; ReplyMessage: {e.ReplyText}"));
            }

        }

        /// <summary>
        /// Convert timeout in second to milisecond and compare with max timeout
        /// </summary>
        /// <param name="timeoutSec"></param>
        /// <returns></returns>
        private static int TimeoutMs(int timeoutSec)
        {
            int timeOutMs = timeoutSec * 1000; //nastaveny timeout
            return timeOutMs > 0 && timeOutMs < MAX_TIMEOUT_MS ? timeOutMs : MAX_TIMEOUT_MS;
        }

        public int ActiveTasks
        {
            get { return callbackMapper.Count; }
        }

    }
}
