﻿using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Communication.Context;
using RabbitMQ.Communication.Contracts;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace RabbitMQ.Communication.Tests.Client
{
    public class RpcPublisherRpcSubscriberTest
    {
        private readonly string _rabbitHostName = "cz03app03.cz.foxconn.com"; //http://cz03app03.cz.foxconn.com:15672 - management  
        private readonly string _userName = "guest";
        private readonly string _password = "guest";
        private readonly string _virtualHost = "test";

        private IModel _chanel = null;
        private IModel CreateChannel()
        {
            if (_chanel == null)
                _chanel = new RabbitMQ.Client.ConnectionFactory() { HostName = _rabbitHostName, UserName = _userName, Password = _password, VirtualHost = _virtualHost }.CreateConnection().CreateModel();

            return _chanel;
        }

        [Fact]
        public async Task DirectAsync()
        {
            string methodname = "RpcPublisherRpcSubscriberTest.DirectAsync";
            IModel channel = CreateChannel();
            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            Func<IMessageContext, BasicDeliverEventArgs, Task<string>> func = async (IMessageContext message1, BasicDeliverEventArgs ea) => await Task.FromResult(message1.Context);

            using (var subscriber = new Communication.Client.RpcSubscriber(channel, methodname + ".a1", func, "amq.direct"))
            {
                using (var publisher = new Communication.Client.RpcPublisher(channel))
                {
                    string receivedMessage = (await publisher.SendAsync(methodname + ".a1", message, exchangeName: "amq.direct")).Context;
                    Assert.Equal(message.Context, receivedMessage);
                }
            }
        }

        [Fact]
        public async Task TopicAsync()
        {
            string methodname = "RpcPublisherRpcSubscriberTest.TopicAsync";
            IModel channel = CreateChannel();
            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            Func<IMessageContext, BasicDeliverEventArgs, Task<string>> func = async (IMessageContext message1, BasicDeliverEventArgs ea) => await Task.FromResult(message1.Context);

            using (var subscriber = new Communication.Client.RpcSubscriber(channel, methodname + ".#", func, "amq.topic"))
            {
                using (var publisher = new Communication.Client.RpcPublisher(channel))
                {
                    string receivedMessage = (await publisher.SendAsync(methodname + ".a1", message, exchangeName: "amq.topic")).Context;
                    Assert.Equal(message.Context, receivedMessage);
                }
            }
        }
    }
}