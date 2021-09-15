using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Communication.Context;
using RabbitMQ.Communication.Contracts;
using RabbitMQ.Communication.Extension;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace RabbitMQ.Communication.Client
{
    public class RpcSubscriber<T> : IDisposable where T: BaseMessageContext
    {
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

        internal Func<T, BasicDeliverEventArgs, CancellationToken, Task<string>> ConsumerFunction { get; }
        internal Subscriber<T> Subscriber { get; }
        internal Publisher Publisher { get; }
        internal IModel Channel { get; }
        private bool DisposeChannel { get; } = false;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connection">Connection</param>
        public RpcSubscriber(IConnection connection, string routingKey, Func<T, BasicDeliverEventArgs, CancellationToken, Task<string>> consumerFunction, string subscriberExchangeName = "amq.topic", ushort? prefetchCount = null) : this(connection.CreateModel(), routingKey, consumerFunction, subscriberExchangeName, prefetchCount)
        {
            // Channel is created in this class please dispose this channel
            DisposeChannel = true;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="channel">Channel</param>
        public RpcSubscriber(IModel channel, string routingKey, Func<T, BasicDeliverEventArgs, CancellationToken, Task<string>> consumerFunction, string subscriberExchangeName = "amq.topic", ushort? prefetchCount = null)
        {            
            Channel = channel;

            Publisher = new Publisher(channel);
            Subscriber = new Subscriber<T>(channel, routingKey, SubscriberFunction, subscriberExchangeName ?? RabbitMQExtension.GetDefaultSubscriberExchangeName, prefetchCount);            
            ConsumerFunction = consumerFunction;
        }

        private async Task SubscriberFunction(T message, BasicDeliverEventArgs ea, CancellationToken ct = default)
        {
            if (ea.BasicProperties.ReplyToAddress == null)
                throw new ArgumentNullException(nameof(ea.BasicProperties.ReplyToAddress));

            BaseResponseMessageContext response = message.GenerateResponse();

            try
            {
                response.Context = await ConsumerFunction(message, ea, ct);
            }
            catch (Exception ex)
            {
                response.Exception = ex;
            }

            await Publisher.SendAsync(ea.BasicProperties.ReplyToAddress.RoutingKey, response, ea.BasicProperties.ReplyToAddress.ExchangeName, ea.BasicProperties.CorrelationId);
        }
    }
}
