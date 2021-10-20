using RabbitMQ.Client;
using RabbitMQ.Communication.Contracts;
using System;

namespace RabbitMQ.Communication.Extension
{
    public static class RabbitMQConnection
    {
        /// <summary>
        /// Create RabbitMQ connection
        /// </summary>
        /// <param name="config"></param>
        /// <param name="additionalTextConnectionName">Additional text for name of connection. Will be added front of class name.</param>
        /// <returns></returns>
        public static IConnection CreateConnection(IRabbitMQConfig config, string additionalTextConnectionName = null)
        {
            try
            {
                var factory = new RabbitMQ.Client.ConnectionFactory() { HostName = config.HostName, UserName = config.UserName, Password = config.Password };
                factory.Port = config.Port ?? factory.Port;
                factory.VirtualHost = config.VirtualHost ?? factory.VirtualHost;
                return factory.CreateConnection(GetConnectionName(additionalTextConnectionName));
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }
        /// <summary>
        /// Connection name
        /// </summary>
        /// <returns></returns>
        private static string GetConnectionName(string additionalText = null)
        {
            additionalText = additionalText ?? string.Empty;
            return additionalText + System.Reflection.Assembly.GetEntryAssembly()?.GetName()?.Name;
        }
    }

}
