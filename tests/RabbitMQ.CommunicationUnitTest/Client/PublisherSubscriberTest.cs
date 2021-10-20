using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Communication.Context;
using RabbitMQ.Communication.Contracts;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace RabbitMQ.Communication.Tests.Client
{
    public class PublisherSubscriberTest
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
            string methodname = "PublisherSubscriberTest.DirectAsync";
            IModel channel = CreateChannel();
            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            BlockingCollection<string> respQueue = new BlockingCollection<string>();

            Func<IMessageContext, BasicDeliverEventArgs, CancellationToken, Task> func = async (IMessageContext message1, BasicDeliverEventArgs ea, CancellationToken ct) =>
                await Task.Run(() => respQueue.Add(message1.Context));

            using (var subscriber = new Communication.Client.Subscriber<BaseMessageContext>(channel, methodname + ".a1", func, "amq.direct"))
            {
                using (var publisher = new Communication.Client.Publisher(channel))
                {
                    await publisher.SendAsync(methodname + ".a1", message, exchangeName: "amq.direct");
                }
            }

            string receivedMessage = respQueue.Take();
            Assert.Equal(message.Context, receivedMessage);

        }

        [Fact]
        public async Task TopicAsync()
        {
            string methodname = "PublisherSubscriberTest.TopicAsync";
            IModel channel = CreateChannel();
            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            BlockingCollection<string> respQueue = new BlockingCollection<string>();
            Func<IMessageContext, BasicDeliverEventArgs, CancellationToken, Task> func = async (IMessageContext message1, BasicDeliverEventArgs ea, CancellationToken ct) =>
                await Task.Run(() => respQueue.Add(message1.Context));

            using (var subscriber = new Communication.Client.Subscriber<BaseMessageContext>(channel, methodname + ".#", func, "amq.topic"))
            {
                using (var publisher = new Communication.Client.Publisher(channel))
                {
                    await publisher.SendAsync(methodname + ".a1", message, exchangeName: "amq.topic");
                }
            }

            string receivedMessage = respQueue.Take();
            Assert.Equal(message.Context, receivedMessage);
        }
    }
}
