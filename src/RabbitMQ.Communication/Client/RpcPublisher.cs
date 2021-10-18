using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Communication.Context;
using RabbitMQ.Communication.Contracts;
using RabbitMQ.Communication.Extension;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

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
                foreach(TaskCompletionSource<BaseResponseMessageContext> t in callbackMapper.Values)
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
            Subscriber = new Subscriber<BaseResponseMessageContext>(channel, RabbitMQExtension.GetDefaultSubscriberRoutingKey, ConsumerFunction, subscriberExchangeName ?? RabbitMQExtension.GetDefaultSubscriberExchangeName, allowCancellation: false);        }

        private Task ConsumerFunction(BaseResponseMessageContext message, BasicDeliverEventArgs ea, CancellationToken ct = default)
        {
            Logger?.LogInformation("RpcPublisher.ConsumerFunction start CorrelationID:" + ea.BasicProperties.CorrelationId, ea.BasicProperties.CorrelationId);
            if (callbackMapper.TryRemove(ea.BasicProperties.CorrelationId, out TaskCompletionSource<BaseResponseMessageContext> tcs))
            {
                if (message.IsError)
                {
                    tcs.SetException(message.Exception);
                    Logger?.LogError(message.Exception, "RpcPublisher.ConsumerFunction Task set exception CorrelationID:" + ea.BasicProperties.CorrelationId, ea.BasicProperties.CorrelationId);
                }
                    
                else
                {
                    tcs.TrySetResult(message);
                    Logger?.LogInformation("RpcPublisher.ConsumerFunction success CorrelationID:" + ea.BasicProperties.CorrelationId, ea.BasicProperties.CorrelationId);
                }                    
            }

            Logger?.LogInformation("RpcPublisher.ConsumerFunction finish CorrelationID:" + ea.BasicProperties.CorrelationId, ea.BasicProperties.CorrelationId);
            return Task.CompletedTask;

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
            if(message == null) 
                throw new ArgumentNullException(nameof(message));

            message.CorrelationID = string.IsNullOrEmpty(message.CorrelationID) ? RabbitMQExtension.GetCorrelationId() : message.CorrelationID;

            if (callbackMapper.ContainsKey(message.CorrelationID))
                throw new DuplicateWaitObjectException(nameof(message.CorrelationID), "A message with this correlation id has already been used. Please generate new ones.");

            try
            {
                TaskCompletionSource<BaseResponseMessageContext> tcs = new TaskCompletionSource<BaseResponseMessageContext>(TaskCreationOptions.RunContinuationsAsynchronously);
                callbackMapper.TryAdd(message.CorrelationID, tcs);

                //setting max timeout
                CancellationTokenSource = new CancellationTokenSource(TimeoutMs(timeoutSec));
                CancellationTokenRegistration timeoutToken = CancellationTokenSource.Token.Register(
                    async () =>
                    {
                        Logger?.LogError("RpcPublisher.SendAsync canceled by timeout CorrelationID:" + message.CorrelationID, message.CorrelationID);
                        tcs.TrySetCanceled();
                        callbackMapper.TryRemove(message.CorrelationID, out var tmp);                        
                        await Publisher.SendAsync(message.CorrelationID, new BaseMessageContext() { Context = "Canceled by timeout" }, exchangeName: "amq.fanout", correlationId: message.CorrelationID);
                    });
                CancellationTokenRegistration userToken = ct.Register(
                    async () =>
                    {
                        Logger?.LogError("RpcPublisher.SendAsync canceled by user CorrelationID:" + message.CorrelationID, message.CorrelationID);
                        tcs.TrySetCanceled();
                        callbackMapper.TryRemove(message.CorrelationID, out var tmp1);
                        await Publisher.SendAsync(message.CorrelationID, new BaseMessageContext() { Context = "Canceled by user" }, exchangeName: "amq.fanout", correlationId: message.CorrelationID);
                    });
                
                Publisher.ReplyTo replyTo = new Publisher.ReplyTo()
                {
                    ExchangeName = Subscriber.ExchangeName,
                    RoutingKey = Subscriber.RoutingKey
                };

                await Publisher.SendAsync(routingKey, message, exchangeName, message.CorrelationID, replyTo, ct, mandatory: true);

                var test = await tcs.Task;

                Logger?.LogInformation("RpcPublisher.SendAsync ready for dispose CorrelationID:" + message.CorrelationID, message.CorrelationID);

                timeoutToken.Dispose();
                userToken.Dispose();

                Logger?.LogInformation("RpcPublisher.SendAsync finish CorrelationID:" + message.CorrelationID, message.CorrelationID);

                return test;

            }
            catch (Exception ex)
            {
                Logger?.LogError(ex, "RpcPublisher.SendAsync catch exception CorrelationID:" + message.CorrelationID, message.CorrelationID);
                if (callbackMapper.TryRemove(message.CorrelationID, out var tmp))                
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
