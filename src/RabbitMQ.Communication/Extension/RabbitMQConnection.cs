using RabbitMQ.Client;
using RabbitMQ.Communication.Contracts;
using System;

namespace RabbitMQ.Communication.Extension
{
    public static class RabbitMQConnection
    {
        public static IConnection CreateConnection(IRabbitMQConfig config)
        {
            try
            {
                var factory = new RabbitMQ.Client.ConnectionFactory() { HostName = config.HostName, UserName = config.UserName, Password = config.Password };
                factory.Port = config.Port ?? factory.Port;
                factory.VirtualHost = config.VirtualHost ?? factory.VirtualHost;
                return factory.CreateConnection(GetConnectionName());
            }
            catch(Exception ex)
            {
                throw ex;
            }

        }
        /// <summary>
        /// Connection name
        /// </summary>
        /// <returns></returns>
        private static string GetConnectionName()
        {
            return System.Reflection.Assembly.GetEntryAssembly()?.GetName()?.Name;
        }
    }

}
