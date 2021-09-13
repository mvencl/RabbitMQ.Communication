using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Communication.Client;
using RabbitMQ.Communication.Context;
using RabbitMQ.Communication.Contracts;
using RabbitMQ.Communication.Extension;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace RabbitMQ.Communication.Tests.Client
{
    public class RpcSubscriberTest
    {
        private readonly string _rabbitHostName = "localhost";
        private readonly string _userName = "guest";
        private readonly string _password = "guest";

        private IModel CreateChannel()
        {
            return new RabbitMQ.Client.ConnectionFactory() { HostName = _rabbitHostName, UserName = _userName, Password = _password }.CreateConnection().CreateModel();
        }

        [Fact]
        public void Direct()
        {
            string queueName = RabbitMQExtension.CleanRoutingKey("RpcSubscriberTest.Direct".ToLower().Trim());

            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            IModel channel = CreateChannel();

            Func<BaseMessageContext, BasicDeliverEventArgs, Task<string>> func = async (BaseMessageContext message1, BasicDeliverEventArgs ea) => 
            await Task.FromResult(message1.Context);
            RpcSubscriber<BaseMessageContext> subscriber = new RpcSubscriber<BaseMessageContext>(channel, queueName, func,"amq.direct");

            BlockingCollection<string> respQueue = new BlockingCollection<string>();
            string responseQueueName = "response." + queueName;
            RpcPublisherMock(channel, responseQueueName, queueName, message, "amq.direct", respQueue);            

            string receivedMessage = respQueue.Take();
            Assert.Equal(message.Context, receivedMessage);
        }

        [Fact]
        public void Topic()
        {
            string queueName = RabbitMQExtension.CleanRoutingKey("RpcSubscriberTest.Topic".ToLower().Trim());

            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            IModel channel = CreateChannel();

            Func<BaseMessageContext, BasicDeliverEventArgs, Task<string>> func = async (BaseMessageContext message1, BasicDeliverEventArgs ea) => await Task.FromResult(message1.Context);
            RpcSubscriber<BaseMessageContext> subscriber = new RpcSubscriber<BaseMessageContext>(channel, queueName + ".#", func, "amq.topic");

            BlockingCollection<string> respQueue = new BlockingCollection<string>();
            string responseQueueName = "response." + queueName;
            RpcPublisherMock(channel, responseQueueName, queueName + ".a1", message, "amq.topic", respQueue);

            string receivedMessage = respQueue.Take();
            Assert.Equal(message.Context, receivedMessage);
        }

        private void RpcPublisherMock(IModel channel, string responseQueueName, string routingKey, IMessageContext message, string publishExchange, BlockingCollection<string> blockingTask)
        {
            //create response queue
            channel.QueueDeclare(responseQueueName, true, false, true);
            channel.QueueBind(responseQueueName, "amq.direct", responseQueueName);

            //create subscriber on service queue
            var consumer = new EventingBasicConsumer(channel);

            consumer.Received += (model, ea) =>
            {
                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                BaseMessageContext response = RabbitMQExtension.DeserializeObject<BaseMessageContext>(ea.Body);
                blockingTask.Add(response.Context);
            };
            channel.BasicConsume(queue: responseQueueName, autoAck: false, consumer: consumer);


            IBasicProperties props = channel.CreateBasicProperties();
            props.CorrelationId = RabbitMQExtension.GetCorrelationId();
            props.ReplyToAddress = new PublicationAddress("direct", "amq.direct", responseQueueName);
            channel.BasicPublish(publishExchange, routingKey, props, RabbitMQExtension.SerializeObject(message));
        }
    }
}
