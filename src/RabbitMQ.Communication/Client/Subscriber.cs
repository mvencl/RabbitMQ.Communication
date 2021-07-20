using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Communication.Contracts;
using RabbitMQ.Communication.Extension;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Communication.Client
{
    public class Subscriber<T> : IDisposable where T:IMessageContext
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

        public Subscriber(IConnection connection, string routingKey, Func<T, BasicDeliverEventArgs, Task> consumerFunction, string subscriberExchangeName = "amq.topic", ushort consumerLimitCount = 1000)
            : this(connection.CreateModel(), routingKey, consumerFunction, subscriberExchangeName)
        {
            DisposeChannel = true;
            Channel.BasicQos(0, consumerLimitCount, false);
        }
        public Subscriber(IModel channel, string routingKey, Func<T, BasicDeliverEventArgs, Task> consumerFunction, string subscriberExchangeName = "amq.topic")
        {
            ExchangeName = subscriberExchangeName;
            RoutingKey = RabbitMQExtension.CleanRoutingKey(routingKey);
            string queueName = RoutingKey;
            Channel = channel;
            
            Channel.ExchangeDeclare(RabbitMQExtension.GetDefaultExchangeName, "topic", true, true);
            Channel.CreateQueue(queueName); 
            Channel.ExchangeBind(RabbitMQExtension.GetDefaultExchangeName, ExchangeName, RoutingKey);
            Channel.QueueBind(queueName, RabbitMQExtension.GetDefaultExchangeName, RoutingKey);

            EventingBasicConsumer consumer = new EventingBasicConsumer(Channel);

            consumer.Received += async (object sender, BasicDeliverEventArgs ea) =>
            {
                Channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                await consumerFunction(RabbitMQExtension.DeserializeObject<T>(ea.Body.Span.ToArray()), ea);
            };
            Channel.BasicConsume(queueName, false, consumer);
        }         
    }
}
