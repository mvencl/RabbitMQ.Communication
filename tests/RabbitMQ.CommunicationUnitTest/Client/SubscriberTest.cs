using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Communication.Context;
using RabbitMQ.Communication.Contracts;
using RabbitMQ.Communication.Extension;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace RabbitMQ.Communication.Tests.Client
{
    public class SubscriberTest
    {
        private readonly string _rabbitHostName = "localhost";
        private readonly string _userName = "guest";
        private readonly string _password = "guest";

        private IModel CreateChannel()
        {
            return new RabbitMQ.Client.ConnectionFactory() { HostName = _rabbitHostName, UserName = _userName, Password = _password }.CreateConnection().CreateModel();
        }

        [Fact]
        public void ListenDirect()
        {
            string methodname = "SubscriberTest.ListenDirect";

            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            IModel channel = CreateChannel();
            IBasicProperties props = channel.CreateBasicProperties();
            props.CorrelationId = RabbitMQExtension.GetCorrelationId();

            BlockingCollection<string> respQueue = new BlockingCollection<string>();

            Func<IMessageContext, BasicDeliverEventArgs, CancellationToken, Task> func = async (IMessageContext message1, BasicDeliverEventArgs ea, CancellationToken ct) => await Task.Run(() => respQueue.Add(message1.Context));

            var subscriber = new Communication.Client.Subscriber<BaseMessageContext>(channel, methodname, func, "amq.direct");

            channel.BasicPublish("amq.direct", RabbitMQExtension.CleanRoutingKey(methodname), props, RabbitMQExtension.SerializeObject(message));

            string receivedMessage = respQueue.Take();
            Assert.Equal(message.Context, receivedMessage);

        }

        /// <summary>
        /// Listen on direct but deserialize to diffrent class with same property - simulate 2 independent services
        /// </summary>
        [Fact]
        public void ListenDirectDifferentClass()
        {
            string methodname = "SubscriberTest.ListenDirectDifferentClass";

            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            IModel channel = CreateChannel();
            IBasicProperties props = channel.CreateBasicProperties();
            props.CorrelationId = RabbitMQExtension.GetCorrelationId();

            BlockingCollection<string> respQueue = new BlockingCollection<string>();

            Func<IMessageContext, BasicDeliverEventArgs, CancellationToken, Task> func = async (IMessageContext message1, BasicDeliverEventArgs ea, CancellationToken ct) =>
            await Task.Run(() => respQueue.Add(message1.Context));

            var subscriber = new Communication.Client.Subscriber<BaseStringMessageContext2>(channel, methodname, func, "amq.direct");

            channel.BasicPublish("amq.direct", RabbitMQExtension.CleanRoutingKey(methodname), props, RabbitMQExtension.SerializeObject(message));

            string receivedMessage = respQueue.Take();
            Assert.Equal(message.Context, receivedMessage);

        }

        [Fact]
        public void ListenTopic()
        {
            string methodname = "SubscriberTest.ListenTopic";

            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            IModel channel = CreateChannel();
            IBasicProperties props = channel.CreateBasicProperties();
            props.CorrelationId = RabbitMQExtension.GetCorrelationId();

            BlockingCollection<string> respQueue = new BlockingCollection<string>();

            Func<IMessageContext, BasicDeliverEventArgs, CancellationToken, Task> func = async (IMessageContext message1, BasicDeliverEventArgs ea, CancellationToken ct) =>
            await Task.Run(() => respQueue.Add(message1.Context));

            var subscriber = new Communication.Client.Subscriber<BaseMessageContext>(channel, $"{methodname}.#", func, "amq.topic");

            channel.BasicPublish("amq.topic", RabbitMQExtension.CleanRoutingKey($"{methodname}.a1") + methodname, props, RabbitMQExtension.SerializeObject(message));

            string receivedMessage = respQueue.Take();
            Assert.Equal(message.Context, receivedMessage);

        }

    }
}
