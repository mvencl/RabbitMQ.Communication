﻿using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Communication.Client;
using RabbitMQ.Communication.Context;
using RabbitMQ.Communication.Contracts;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading;
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

            Func<IMessageContext, BasicDeliverEventArgs, CancellationToken, Task<string>> func = async (IMessageContext message1, BasicDeliverEventArgs ea, CancellationToken ct) => await Task.FromResult(message1.Context);

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

            Func<IMessageContext, BasicDeliverEventArgs, CancellationToken, Task<string>> func = async (IMessageContext message1, BasicDeliverEventArgs ea, CancellationToken ct) => await Task.FromResult(message1.Context);

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

            Func<IMessageContext, BasicDeliverEventArgs, CancellationToken, Task<string>> func = (IMessageContext message1, BasicDeliverEventArgs ea, CancellationToken ct) => throw new Exception("Error");

            using (var subscriber = new Communication.Client.RpcSubscriber<BaseMessageContext>(channel, methodname + ".#", func, "amq.topic"))
            {
                using (var publisher = new Communication.Client.RpcPublisher(channel))
                {
                    await Assert.ThrowsAsync<Exception>(() => publisher.SendAsync(methodname + ".a1", message, exchangeName: "amq.topic"));
                }
            }
        }

        [Fact]
        public async Task ExceptionTypeAsync()
        {
            string methodname = "RpcPublisherRpcSubscriberTest.ExceptionTypeAsync";
            IModel channel = CreateChannel();
            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            Func<IMessageContext, BasicDeliverEventArgs, CancellationToken, Task<string>> func = (IMessageContext message1, BasicDeliverEventArgs ea, CancellationToken ct) => throw new ValidationException("Validation");

            using (var subscriber = new Communication.Client.RpcSubscriber<BaseMessageContext>(channel, methodname + ".#", func, "amq.topic"))
            {
                using (var publisher = new Communication.Client.RpcPublisher(channel))
                {
                    await Assert.ThrowsAsync<ValidationException>(() => publisher.SendAsync(methodname + ".a1", message, exchangeName: "amq.topic"));
                }
            }
        }

        [Fact]
        public void CancelRPCCallAsnyc()
        {
            string methodname = "RpcPublisherRpcSubscriberTest.CancelRPCCallAsnyc";
            IModel channel = CreateChannel();

            BlockingCollection<bool> blockingTask1 = new BlockingCollection<bool>();
            BlockingCollection<bool> blockingTask2 = new BlockingCollection<bool>();

            new Thread(() =>
            {
                Func<IMessageContext, BasicDeliverEventArgs, CancellationToken, Task<string>> func = async (IMessageContext message1, BasicDeliverEventArgs ea, CancellationToken ct) =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(6));
                    if (message1.Context == "blockingTask1")
                        blockingTask1.Add(ct.IsCancellationRequested);
                    if (message1.Context == "blockingTask2")
                        blockingTask2.Add(ct.IsCancellationRequested);
                    return await Task.FromResult(message1.Context);

                };
                RpcSubscriber<BaseMessageContext> subscriber = new Communication.Client.RpcSubscriber<BaseMessageContext>(channel, methodname + ".#", func, "amq.topic");
            }).Start();

            // message to be canceled
            new Thread(async () =>
            {
                RpcPublisher publisher = new RpcPublisher(channel);
                CancellationTokenSource cts = new CancellationTokenSource();
                IMessageContext message = new BaseMessageContext() { Context = "blockingTask1" };
                new Thread(
                    () => Assert.Throws<AggregateException>(publisher.SendAsync(methodname + ".a1", message, exchangeName: "amq.topic", ct: cts.Token).Wait)
                        ).Start();
                await Task.Delay(TimeSpan.FromSeconds(1));
                cts.Cancel();
            }).Start();

            // message that should not be canceled
            new Thread(() =>
            {
                RpcPublisher publisher = new RpcPublisher(channel);
                CancellationTokenSource cts = new CancellationTokenSource();
                IMessageContext message = new BaseMessageContext() { Context = "blockingTask2" };
                new Thread(
                    async () => await publisher.SendAsync(methodname + ".a1", message, exchangeName: "amq.topic", ct: cts.Token)
                            ).Start();
            }).Start();

            bool wasCancelled1 = blockingTask1.Take();
            bool wasCancelled2 = blockingTask2.Take();
            Assert.True(wasCancelled1);
            Assert.False(wasCancelled2);
        }
    }
}
