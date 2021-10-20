using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Communication.Context;
using RabbitMQ.Communication.Contracts;
using RabbitMQ.Communication.Extension;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using Xunit;

namespace RabbitMQ.Communication.Tests.Client
{
    public class PublisherTest
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
        public async Task PublishDirectAsync()
        {
            string methodname = "PublisherTest.PublishDirectAsync";

            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            IModel channel = CreateChannel();

            BlockingCollection<string> respQueue = new BlockingCollection<string>();
            #region mock service 

            methodname = methodname.ToLower().Trim();
            //create service queue
            channel.QueueDeclare(methodname, true, false, true);
            channel.QueueBind(methodname, "amq.direct", methodname);

            //create subscriber on service queue
            var consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queue: methodname, autoAck: false, consumer: consumer);
            consumer.Received += (model, ea) =>
            {
                respQueue.Add(RabbitMQExtension.DeserializeObject<BaseMessageContext>(ea.Body).Context);
                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            };
            #endregion mock service  

            var publisher = new Communication.Client.Publisher(channel);
            await publisher.SendAsync(methodname, message, exchangeName: "amq.direct");

            string receivedMessage = respQueue.Take();
            Assert.Equal(message.Context, receivedMessage);

        }

        /// <summary>
        /// Listen on direct but deserialize to diffrent class with same property - simulate 2 independent services
        /// </summary>
        [Fact]
        public async Task PublishDirectDifferentClassAsync()
        {
            string methodname = "PublisherTest.PublishDirectDifferentClassAsync";

            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            IModel channel = CreateChannel();

            BlockingCollection<string> respQueue = new BlockingCollection<string>();
            #region mock service 

            methodname = methodname.ToLower().Trim();
            //create service queue
            channel.QueueDeclare(methodname, true, false, true);
            channel.QueueBind(methodname, "amq.direct", methodname);

            //create subscriber on service queue
            var consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queue: methodname, autoAck: false, consumer: consumer);
            consumer.Received += (model, ea) =>
            {
                respQueue.Add(RabbitMQExtension.DeserializeObject<BaseStringMessageContext2>(ea.Body).Context);
                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            };
            #endregion mock service  

            var publisher = new Communication.Client.Publisher(channel);
            await publisher.SendAsync(methodname, message, exchangeName: "amq.direct");

            string receivedMessage = respQueue.Take();
            Assert.Equal(message.Context, receivedMessage);

        }

        [Fact]
        public async Task PublishTopicAsnyc()
        {
            string methodname = "PublisherTest.PublishTopicAsnyc";

            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            IModel channel = CreateChannel();

            BlockingCollection<string> respQueue = new BlockingCollection<string>();
            #region mock service 

            methodname = methodname + ".#";
            methodname = methodname.ToLower().Trim();
            //create service queue
            channel.QueueDeclare(methodname, true, false, true);
            channel.QueueBind(methodname, "amq.topic", methodname);

            //create subscriber on service queue
            var consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queue: methodname, autoAck: false, consumer: consumer);
            consumer.Received += (model, ea) =>
            {
                respQueue.Add(RabbitMQExtension.DeserializeObject<BaseMessageContext>(ea.Body).Context);
                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            };
            #endregion mock service  

            var publisher = new Communication.Client.Publisher(channel);
            await publisher.SendAsync(methodname + ".a1", message, exchangeName: "amq.topic");

            string receivedMessage = respQueue.Take();
            Assert.Equal(message.Context, receivedMessage);

        }

    }
}
