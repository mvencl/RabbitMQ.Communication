using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Communication.Contracts;
using RabbitMQ.Communication.Extension;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Communication.Client
{
    public class Subscriber<T> : IDisposable where T : IMessageContext
    {
        #region Dispose
        private bool DisposeChannel { get; } = false;
        private bool disposedValue;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)                    
                    if (DisposeChannel) Channel.Dispose();
                }
                CancellationTokenSource.Clear();
                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        // ~Subscriber()
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

        /// <summary>
        /// Routing key on whitch will listening
        /// </summary>
        public string RoutingKey { get; }

        /// <summary>
        /// Exchange name on which will listening (queue will be binding)
        /// </summary>
        public string ExchangeName { get; }

        /// <summary>
        /// Channel
        /// </summary>
        internal IModel Channel { get; }
        internal ILogger Logger { get; }

        public Subscriber(IConnection connection, string routingKey, Func<T, BasicDeliverEventArgs, CancellationToken, Task> consumerFunction, string subscriberExchangeName = "amq.topic", ushort? prefetchCount = null, bool allowCancellation = true, ILogger logger = null)
            : this(connection.CreateModel(), routingKey, consumerFunction, subscriberExchangeName, prefetchCount, allowCancellation, logger)
        {
            DisposeChannel = true;
        }
        public Subscriber(IModel channel, string routingKey, Func<T, BasicDeliverEventArgs, CancellationToken, Task> consumerFunction, string subscriberExchangeName = "amq.topic", ushort? prefetchCount = null, bool allowCancellation = true, ILogger logger = null)
        {
            ExchangeName = subscriberExchangeName;
            RoutingKey = RabbitMQExtension.CleanRoutingKey(routingKey);
            string queueName = RoutingKey;
            Channel = channel;

            if (prefetchCount != null)
                Channel.BasicQos(0, prefetchCount.Value, false);

            if (allowCancellation)
                CancelQueue(queueName);

            Channel.CreateQueue(queueName);
            Channel.QueueBind(queueName, ExchangeName, RoutingKey);

            EventingBasicConsumer consumer = new EventingBasicConsumer(Channel);

            if (Channel.DefaultConsumer == null)
                Channel.DefaultConsumer = consumer;

            consumer.Received += async (object sender, BasicDeliverEventArgs ea) =>
            {
                try
                {
                    CancellationTokenSource cts = new CancellationTokenSource();
                    CancellationTokenSource.Add(ea.BasicProperties.CorrelationId, cts);

                    await consumerFunction(RabbitMQExtension.DeserializeObject<T>(ea.Body.Span.ToArray()), ea, cts.Token);

                    Channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                }
                catch (Exception ex)
                {
                    Logger?.LogError(ex, "Subscriber.Received ended with exception correlationId:{correlationId}", ea.BasicProperties.CorrelationId);
                    throw ex;
                }

            };
            Channel.BasicConsume(queueName, false, consumer);
        }

        private Dictionary<string, CancellationTokenSource> CancellationTokenSource = new Dictionary<string, CancellationTokenSource>();

        private void CancelQueue(string queueName)
        {
            string cancelQueueName = "Cancelation:" + queueName + Guid.NewGuid().ToString();
            Channel.CreateQueue(cancelQueueName, true);
            Channel.QueueBind(cancelQueueName, "amq.fanout", "");
            EventingBasicConsumer consumer = new EventingBasicConsumer(Channel);
            consumer.Received += (object sender, BasicDeliverEventArgs ea) =>
            {
                Logger?.LogError("Subscriber.CancelQueue A cancellation message was received for correlationId:{correlationId}", ea.BasicProperties.CorrelationId);
                if (CancellationTokenSource.TryGetValue(ea.BasicProperties.CorrelationId, out CancellationTokenSource cts))
                    cts.Cancel();
                Channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            };
            Channel.BasicConsume(cancelQueueName, false, consumer);
        }

    }
}
