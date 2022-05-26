using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Communication.Contracts;
using RabbitMQ.Communication.Extension;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.Communication.Client
{
    public class Publisher : IDisposable
    {
        #region Dispose
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
        // ~Publisher()
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


        //uložení zpráv na disk --- durable musí být všude 
        //Queue alesepon na 2 nodech
        //    Federaion na gateway rabbitmq
        // Cancalation Queue přehodit na streaming stačí potom jen jedna queue



        internal IModel Channel { get; }
        private bool DisposeChannel { get; } = false;
        internal ILogger Logger { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connection">Connection</param>
        public Publisher(IConnection connection, ILogger logger = null) : this(connection.CreateModel(), logger)
        {
            // Channel is created in this class please dispose this channel
            DisposeChannel = true;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="channel">Channel</param>
        public Publisher(IModel channel, ILogger logger = null)
        {
            Channel = channel;
            Logger = logger;
        }

        /// <summary>
        /// Function for sending a message for which we are not waiting for a reply.
        /// </summary>
        /// <param name="requestRoutingKey">Routing key to use for sending the message. For separation, it must contain dots no slashes.</param>
        /// <param name="message">Message to be sent.</param>
        /// <param name="publisherExchangeName">If not set, the value from the PublisherExchangeNameConfig property is used.</param>
        public Task SendAsync(string routingKey, IMessageContext message, string exchangeName = "amq.topic", string correlationId = null, ReplyTo replyTo = null, CancellationToken ct = default, bool mandatory = false)
        {
            if (string.IsNullOrEmpty(routingKey))
                throw new ArgumentNullException(nameof(routingKey));
            if (string.IsNullOrEmpty(exchangeName))
                throw new ArgumentNullException(nameof(exchangeName));
            if (message == null)
                throw new ArgumentNullException(nameof(message));

            routingKey = RabbitMQExtension.CleanRoutingKey(routingKey);
            correlationId = correlationId ?? RabbitMQExtension.GetCorrelationId();

            IBasicProperties props = Channel.CreateBasicProperties();
            props.CorrelationId = correlationId;

            if (replyTo != null)
            {
                props.ReplyToAddress = new PublicationAddress(RabbitMQExtension.GetDefaultSubscriberExchangeName, replyTo.ExchangeName, replyTo.RoutingKey);
            }

            Logger?.LogDebug("Message with correlationId:{correlationId} was sended to {Exchange} -> {RoutingKey}", correlationId, exchangeName, routingKey);

            Channel.BasicPublish(exchangeName, routingKey, mandatory, props, RabbitMQExtension.SerializeObject(message));

            return Task.CompletedTask;
        }



        /// <summary>
        /// Reply to configuration class
        /// </summary>
        public class ReplyTo
        {
            /// <summary>
            /// Reply to routing key
            /// </summary>
            public string RoutingKey { get; set; }

            /// <summary>
            /// Reply to exchange name
            /// </summary>
            public string ExchangeName { get; set; }
        }

    }
}
