﻿using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Communication.Context;
using RabbitMQ.Communication.Contracts;
using RabbitMQ.Communication.Extension;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace RabbitMQ.Communication.Tests.Client
{
    public class RpcPublisherTest : IDisposable
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
            string queueName = RabbitMQExtension.CleanRoutingKey("RpcPublisherTest.DirectAsync".ToLower().Trim());

            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            IModel channel = CreateChannel();

            RpcSubscriberTest(channel, queueName, "amq.direct");

            using (var publisher = new Communication.Client.RpcPublisher(channel))
            {
                IMessageContext receivedMessage = await publisher.SendAsync(queueName, message, exchangeName: "amq.direct");
                Assert.Equal(message.Context, receivedMessage.Context);
                Assert.Equal(0, publisher.ActiveTasks);
            }
        }

        [Fact]
        public async Task TopicAsync()
        {
            string queueName = RabbitMQExtension.CleanRoutingKey("RpcPublisherTest.TopicAsync".ToLower().Trim());

            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            IModel channel = CreateChannel();

            RpcSubscriberTest(channel, queueName + ".#", "amq.topic");

            using (var publisher = new Communication.Client.RpcPublisher(channel))
            {
                IMessageContext receivedMessage = await publisher.SendAsync(queueName + ".a1", message, exchangeName: "amq.topic");
                Assert.Equal(message.Context, receivedMessage.Context);
                Assert.Equal(0, publisher.ActiveTasks);
            }
        }

        [Fact]
        public void TopicParallelRunAsync()
        {
            int iteration = 1000;

            string queueName = RabbitMQExtension.CleanRoutingKey("RpcPublisherTest.TopicParallelRunAsync".ToLower().Trim());

            IMessageContext message = new BaseMessageContext() { Context = "This is route" };

            IModel channel = CreateChannel();

            RpcSubscriberTest(channel, queueName + ".#", "amq.topic");

            using (var publisher = new Communication.Client.RpcPublisher(channel))
            {
                List<Task<BaseResponseMessageContext>> tasks = new List<Task<BaseResponseMessageContext>>();

                for (int i = 0; i < iteration; i++)
                {
                    tasks.Add(publisher.SendAsync($"{queueName}.a{i}", message, exchangeName: "amq.topic"));
                }

                Task.WaitAll(tasks.ToArray());

                Assert.Equal(iteration, tasks.Where(t => t.IsCompleted).Count());
                Assert.Single(tasks.GroupBy(t => t.Result.Context));
                Assert.Equal(message.Context, tasks.GroupBy(t => t.Result.Context).FirstOrDefault().Key);
                Assert.Equal(0, publisher.ActiveTasks);
            }
        }

        [Fact]
        public void Cancel()
        {
            string queueName = RabbitMQExtension.CleanRoutingKey("RpcPublisherTest.CancelAsync".ToLower().Trim());

            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            IModel channel = CreateChannel();

            RpcSubscriberTest(channel, queueName, "amq.direct");

            using (var publisher = new Communication.Client.RpcPublisher(channel))
            {
                CancellationTokenSource source = new CancellationTokenSource();
                Task<BaseResponseMessageContext> receivedMessage = publisher.SendAsync(queueName, message, exchangeName: "amq.direct", ct: source.Token);
                source.Cancel();
                Assert.Throws<AggregateException>(() => { receivedMessage.Wait(); });
                Assert.True(receivedMessage.IsCanceled);
                Assert.Equal(0, publisher.ActiveTasks);
            }
        }

        [Fact]
        public async Task NoRouteFound()
        {
            using (var publisher = new Communication.Client.RpcPublisher(CreateChannel()))
            {
                await Assert.ThrowsAsync<MissingMethodException>(() => publisher.SendAsync("NotFound", new BaseMessageContext()));
            }
        }

        private void RpcSubscriberTest(IModel channel, string queueName, string exchangeName)
        {
            //create service queue
            channel.QueueDeclare(queueName, true, false, true);
            channel.QueueBind(queueName, exchangeName, queueName);

            //create subscriber on service queue
            var consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);
            consumer.Received += (model, ea) =>
            {
                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

                BaseMessageContext request = RabbitMQExtension.DeserializeObject<BaseMessageContext>(ea.Body);
                BaseMessageContext response = new BaseMessageContext(request.Context);

                IBasicProperties props = channel.CreateBasicProperties();
                props.CorrelationId = ea.BasicProperties.CorrelationId;
                channel.BasicPublish(ea.BasicProperties.ReplyToAddress.ExchangeName, ea.BasicProperties.ReplyToAddress.RoutingKey, props, RabbitMQExtension.SerializeObject(response));
            };

        }

        public void Dispose()
        {
            _chanel.Dispose();
        }
    }
}
