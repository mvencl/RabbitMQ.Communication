using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Communication.Context;
using RabbitMQ.Communication.Contracts;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace RabbitMQ.Communication.Tests.Client
{
    public class RpcPublisherRpcSubscriberTest
    {
        private readonly string _rabbitHostName = "localhost";
        private readonly string _userName = "guest";
        private readonly string _password = "guest";

        private IModel _chanel = null;
        private IModel CreateChannel()
        {
            if (_chanel == null)
                _chanel = new RabbitMQ.Client.ConnectionFactory() { HostName = _rabbitHostName, UserName = _userName, Password = _password }.CreateConnection().CreateModel();

            return _chanel;
        }

        [Fact]
        public async Task DirectAsync()
        {
            string methodname = "RpcPublisherRpcSubscriberTest.DirectAsync";
            IModel channel = CreateChannel();
            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            Func<IMessageContext, BasicDeliverEventArgs, Task<string>> func = async (IMessageContext message1, BasicDeliverEventArgs ea) => await Task.FromResult(message1.Context);

            using (var subscriber = new Communication.Client.RpcSubscriber<BaseMessageContext>(channel, methodname + ".a1", func, "amq.direct"))
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

            using (var subscriber = new Communication.Client.RpcSubscriber<BaseMessageContext>(channel, methodname + ".#", func, "amq.topic"))
            {
                using (var publisher = new Communication.Client.RpcPublisher(channel))
                {
                    string receivedMessage = (await publisher.SendAsync(methodname + ".a1", message, exchangeName: "amq.topic")).Context;
                    Assert.Equal(message.Context, receivedMessage);
                }
            }
        }

        [Fact]
        public async Task ExceptionAsync()
        {
            string methodname = "RpcPublisherRpcSubscriberTest.ExceptionAsync";
            IModel channel = CreateChannel();
            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            Func<IMessageContext, BasicDeliverEventArgs, Task<string>> func = (IMessageContext message1, BasicDeliverEventArgs ea) => throw new Exception("Error");

            using (var subscriber = new Communication.Client.RpcSubscriber<BaseMessageContext>(channel, methodname + ".#", func, "amq.topic"))
            {
                using (var publisher = new Communication.Client.RpcPublisher(channel))
                {                    
                    BaseResponseMessageContext receivedMessage = (await publisher.SendAsync(methodname + ".a1", message, exchangeName: "amq.topic"));
                    Assert.True(receivedMessage.IsError);
                }
            }
        }

        [Fact]
        public async Task ExceptionTypeAsync()
        {
            string methodname = "RpcPublisherRpcSubscriberTest.ExceptionAsync";
            IModel channel = CreateChannel();
            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            Func<IMessageContext, BasicDeliverEventArgs, Task<string>> func = (IMessageContext message1, BasicDeliverEventArgs ea) => throw new ValidationException("Validation");

            using (var subscriber = new Communication.Client.RpcSubscriber<BaseMessageContext>(channel, methodname + ".#", func, "amq.topic"))
            {
                using (var publisher = new Communication.Client.RpcPublisher(channel))
                {
                    BaseResponseMessageContext receivedMessage = await publisher.SendAsync(methodname + ".a1", message, exchangeName: "amq.topic");
                    Assert.Throws<ValidationException>(() => MethodThatThrows(receivedMessage));
                }
            }
        }
        void MethodThatThrows(BaseResponseMessageContext message)
        {
            throw message.Exception;
        }                
    }
}
