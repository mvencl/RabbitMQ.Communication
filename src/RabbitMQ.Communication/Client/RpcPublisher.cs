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
                if (disposing)
                {
                    Publisher.Dispose();
                    Subscriber.Dispose();
                    // TODO: dispose managed state (managed objects)
                    if (DisposeChannel) Channel.Dispose();
                }
                // TODO: free unmanaged resources (unmanaged objects) and override finalizer                
                foreach(TaskCompletionSource<BaseMessageContext> t in callbackMapper.Values)
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
                
        internal Subscriber<BaseMessageContext> Subscriber { get; }
        internal Publisher Publisher { get; }
        internal IModel Channel { get; }
        private bool DisposeChannel { get; } = false;

        private readonly ConcurrentDictionary<string, TaskCompletionSource<BaseMessageContext>> callbackMapper =
                new ConcurrentDictionary<string, TaskCompletionSource<BaseMessageContext>>();

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connection">Connection</param>
        public RpcPublisher(IConnection connection, string subscriberExchangeName = "amq.direct") : this(connection.CreateModel(), subscriberExchangeName)
        {
            // Channel is created in this class please dispose this channel
            DisposeChannel = true;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="channel">Channel</param>
        public RpcPublisher(IModel channel, string subscriberExchangeName = "amq.direct")
        {
            Channel = channel;
            Publisher = new Publisher(channel);
            Subscriber = new Subscriber<BaseMessageContext>(channel, RabbitMQExtension.GetDefaultSubscriberRoutingKey, ConsumerFunction, subscriberExchangeName ?? RabbitMQExtension.GetDefaultSubscriberExchangeName);
        }

        private async Task ConsumerFunction(BaseMessageContext message, BasicDeliverEventArgs ea)
        {
            if (!callbackMapper.TryRemove(ea.BasicProperties.CorrelationId, out TaskCompletionSource<BaseMessageContext> tcs))
                return;
            await Task.Run(() => tcs.TrySetResult(message));
        }
            

        /// <summary>
        /// Function for sending a message for which we are not waiting for a reply.
        /// </summary>
        /// <param name="requestRoutingKey">Routing key to use for sending the message. For separation, it must contain dots no slashes.</param>
        /// <param name="message">Message to be sent.</param>
        /// <param name="publisherExchangeName">If not set, the value from the PublisherExchangeNameConfig property is used.</param>
        public async Task<IMessageContext> SendAsync(string routingKey, IMessageContext message, int timeoutSec = 0, string exchangeName = "amq.topic", CancellationToken ct = default)
        {
            string correlationId = RabbitMQExtension.GetCorrelationId();

            try
            {
                message.CorrelationID = correlationId;

                TaskCompletionSource<BaseMessageContext> tcs = new TaskCompletionSource<BaseMessageContext>(TaskCreationOptions.RunContinuationsAsynchronously);
                callbackMapper.TryAdd(correlationId, tcs);

                //setting max timeout
                new CancellationTokenSource(TimeoutMs(timeoutSec)).Token.Register(
                    () =>
                    {
                        tcs.TrySetCanceled();
                        callbackMapper.TryRemove(correlationId, out var tmp);
                    });
                ct.Register(
                    () =>
                    {
                        tcs.TrySetCanceled();
                        callbackMapper.TryRemove(correlationId, out var tmp1);
                    });

                Publisher.ReplyTo replyTo = new Publisher.ReplyTo()
                {
                    ExchangeName = Subscriber.ExchangeName,
                    RoutingKey = Subscriber.RoutingKey
                };

                await Publisher.SendAsync(routingKey, message, exchangeName, correlationId, replyTo, ct);

                return await tcs.Task;                

            }
            catch
            {
                callbackMapper.TryRemove(correlationId, out var tmp);
                throw;
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
