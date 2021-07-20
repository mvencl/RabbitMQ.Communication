using RabbitMQ.Client;
using RabbitMQ.Client.Events;
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
    public class RpcPublisherTest: IDisposable
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




        // Otestovat paralelní běh
        // Otestovat Cancel
        // Otestovat jestli se čistí kolekce tasku






        [Fact]
        public async Task DirectAsync()
        {
            string queueName = RabbitMQExtension.CleanRoutingKey("RpcPublisherTest.DirectAsync".ToLower().Trim());

            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            IModel channel = CreateChannel();

            #region mock service 

            
            //create service queue
            channel.QueueDeclare(queueName, true, false, true);
            channel.QueueBind(queueName, "amq.direct", queueName);

            //create subscriber on service queue
            var consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);
            consumer.Received += (model, ea) =>
            {                
                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                BaseMessageContext request = RabbitMQExtension.DeserializeObject<BaseMessageContext>(ea.Body);
                BaseMessageContext response = request.GenerateResponse(request.Context);

                IBasicProperties props = channel.CreateBasicProperties();
                props.CorrelationId = ea.BasicProperties.CorrelationId;
                channel.BasicPublish(ea.BasicProperties.ReplyToAddress.ExchangeName, ea.BasicProperties.ReplyToAddress.RoutingKey, props, RabbitMQExtension.SerializeObject(response));
            };
            #endregion mock service  

            using (var publisher = new Communication.Client.RpcPublisher(channel))
            {
                IMessageContext receivedMessage = await publisher.SendAsync(queueName, message, exchangeName: "amq.direct");
                Assert.Equal(message.Context, receivedMessage.Context);
            }
        }

        [Fact]
        public async Task TopicAsync()
        {
            string queueName = RabbitMQExtension.CleanRoutingKey("RpcPublisherTest.TopicAsync".ToLower().Trim());

            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            IModel channel = CreateChannel();

            #region mock service 

            
            //create service queue
            channel.QueueDeclare(queueName + ".#", true, false, true);
            channel.QueueBind(queueName + ".#", "amq.topic", queueName + ".#");

            //create subscriber on service queue
            var consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queue: queueName + ".#", autoAck: false, consumer: consumer);
            consumer.Received += (model, ea) =>
            {
                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                BaseMessageContext request = RabbitMQExtension.DeserializeObject<BaseMessageContext>(ea.Body);
                BaseMessageContext response = request.GenerateResponse(request.Context);

                IBasicProperties props = channel.CreateBasicProperties();
                props.CorrelationId = ea.BasicProperties.CorrelationId;
                channel.BasicPublish(ea.BasicProperties.ReplyToAddress.ExchangeName, ea.BasicProperties.ReplyToAddress.RoutingKey, props, RabbitMQExtension.SerializeObject(response));
            };
            #endregion mock service  

            using (var publisher = new Communication.Client.RpcPublisher(channel))
            {
                IMessageContext receivedMessage = await publisher.SendAsync(queueName + ".a1", message, exchangeName: "amq.topic");
                Assert.Equal(message.Context, receivedMessage.Context);
            }
        }

        [Fact]
        public void TopicParallelRunAsync()
        {
            string queueName = RabbitMQExtension.CleanRoutingKey("RpcPublisherTest.TopicParallelRunAsync".ToLower().Trim());

            IMessageContext message = new BaseMessageContext() { Context = "This is route" };

            IModel channel = CreateChannel();

            #region mock service 

            
            //create service queue
            channel.QueueDeclare(queueName + ".#", true, false, true);
            channel.QueueBind(queueName + ".#", "amq.topic", queueName + ".#");

            //create subscriber on service queue
            var consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queue: queueName + ".#", autoAck: false, consumer: consumer);
            consumer.Received += Consumer_Received;
            #endregion mock service  

            using (var publisher = new Communication.Client.RpcPublisher(channel))
            {
                Task<IMessageContext> task1 = publisher.SendAsync(queueName + ".a1", message, exchangeName: "amq.topic");
                Task<IMessageContext> task2 = publisher.SendAsync(queueName + ".a2", message, exchangeName: "amq.topic");

                Task.WaitAll(task1, task2);

                Assert.Equal(message.Context + ".a1", task1.Result.Context);
                Assert.Equal(message.Context + ".a2", task2.Result.Context);
            }
        }

        private void Consumer_Received(object channel, BasicDeliverEventArgs e)
        {
            ((IModel)channel).BasicAck(deliveryTag: e.DeliveryTag, multiple: false);

            BaseMessageContext request = RabbitMQExtension.DeserializeObject<BaseMessageContext>(e.Body);
            BaseMessageContext response = request.GenerateResponse(request.Context + e.RoutingKey.Substring(e.RoutingKey.LastIndexOf('.')));

            IBasicProperties props = ((IModel)channel).CreateBasicProperties();
            props.CorrelationId = e.BasicProperties.CorrelationId;
            ((IModel)channel).BasicPublish(e.BasicProperties.ReplyToAddress.ExchangeName, e.BasicProperties.ReplyToAddress.RoutingKey, props, RabbitMQExtension.SerializeObject(response));
        }
       
        public void Dispose()
        {
            _chanel.Dispose();
        }
    }
}
