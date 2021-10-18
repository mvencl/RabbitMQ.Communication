using RabbitMQ.Client;
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

        [Fact]
        public Task AsyncCheckTypeAsync()
        {
            string methodname = "RpcPublisherRpcSubscriberTest.AsyncCheckTypeAsync";
            IModel channel = CreateChannel();            

            Func<IMessageContext, BasicDeliverEventArgs, CancellationToken, Task<string>> func = async (message1, ea, ct) => 
                {
                    await Task.Delay(TimeSpan.FromSeconds(10), ct);
                    return await Task.FromResult(message1.Context);
                };

            DateTime start = DateTime.Now;
            DateTime end = DateTime.Now;

            using (var subscriber = new RpcSubscriber<BaseMessageContext>(channel, methodname + ".#", func, "amq.topic"))
            {
                using (var publisher = new RpcPublisher(channel))
                {
                    Task.WaitAll(
                        publisher.SendAsync(methodname + ".a1", new BaseMessageContext() { Context = "This is it 1" }, exchangeName: "amq.topic"),
                        publisher.SendAsync(methodname + ".a1", new BaseMessageContext() { Context = "This is it 2" }, exchangeName: "amq.topic")
                        );
                    end = DateTime.Now;
                }
            }

            Assert.True((end - start).Seconds < 20);
            return Task.CompletedTask;
        }

        [Fact]
        public async Task CorrelationIdTestAsync()
        {
            string methodname = "RpcPublisherRpcSubscriberTest.CorrelationIdTestAsync";
            IModel channel = CreateChannel();
            IMessageContext message = new BaseMessageContext() { Context = "This is it" };

            Func<IMessageContext, BasicDeliverEventArgs, CancellationToken, Task<string>> func = async (IMessageContext message1, BasicDeliverEventArgs ea, CancellationToken ct) => 
            await Task.FromResult(message1.Context);

            using (var subscriber = new RpcSubscriber<BaseMessageContext>(channel, methodname + ".#", func, "amq.topic"))
            {
                using (var publisher = new RpcPublisher(channel))
                {
                    BaseResponseMessageContext receivedMessage = await publisher.SendAsync(methodname + ".a1", message, exchangeName: "amq.topic");
                    Assert.Equal(message.CorrelationID, receivedMessage.CorrelationID);
                }
            }
        }

        [Fact]
        public async Task ManualCorrelationIdTestAsync()
        {
            string methodname = "RpcPublisherRpcSubscriberTest.CorrelationIdTestAsync";
            IModel channel = CreateChannel();

            string manualcorrelationId = Guid.NewGuid().ToString();

            IMessageContext message = new BaseMessageContext() { Context = "This is it", CorrelationID = manualcorrelationId };

            Func<IMessageContext, BasicDeliverEventArgs, CancellationToken, Task<string>> func = async (IMessageContext message1, BasicDeliverEventArgs ea, CancellationToken ct) =>
            await Task.FromResult(message1.Context);

            using (var subscriber = new RpcSubscriber<BaseMessageContext>(channel, methodname + ".#", func, "amq.topic"))
            {
                using (var publisher = new RpcPublisher(channel))
                {
                    BaseResponseMessageContext receivedMessage = await publisher.SendAsync(methodname + ".a1", message, exchangeName: "amq.topic");
                    Assert.Equal(manualcorrelationId, receivedMessage.CorrelationID);
                }
            }
        }

        [Fact]
        public void PerformanceTestAsync()
        {
            string methodname = "RpcPublisherRpcSubscriberTest.DirectAsync";
            IModel channel = CreateChannel();           

            Func<IMessageContext, BasicDeliverEventArgs, CancellationToken, Task<string>> func = async (IMessageContext message1, BasicDeliverEventArgs ea, CancellationToken ct) =>
            {
                int delay = new Random().Next(10, 1000);
                //await Task.Delay(delay);
                return await Task.FromResult(message1.Context);
            };

            using (var subscriber = new RpcSubscriber<BaseMessageContext>(channel, methodname + ".a1", func, "amq.direct"))
            {
                using (var publisher = new RpcPublisher(channel))
                {
                    for(int i =0; i < 10000; i++)
                    {
                        IMessageContext message1 = new BaseMessageContext() { Context = "This is it task 1" };
                        IMessageContext message2 = new BaseMessageContext() { Context = "This is it task 2" };
                        IMessageContext message3 = new BaseMessageContext() { Context = "This is it task 3" };
                        IMessageContext message4 = new BaseMessageContext() { Context = "This is it task 4" };
                        IMessageContext message5 = new BaseMessageContext() { Context = "This is it task 5" };
                        IMessageContext message6 = new BaseMessageContext() { Context = "This is it task 6" };
                        IMessageContext message7 = new BaseMessageContext() { Context = "This is it task 7" };
                        IMessageContext message8 = new BaseMessageContext() { Context = "This is it task 8" };
                        IMessageContext message9 = new BaseMessageContext() { Context = "This is it task 9" };
                        IMessageContext message10 = new BaseMessageContext() { Context = "This is it task 10" };

                        Task<BaseResponseMessageContext> task1 = publisher.SendAsync(methodname + ".a1", message1, exchangeName: "amq.direct");
                        Task<BaseResponseMessageContext> task2 = publisher.SendAsync(methodname + ".a1", message2, exchangeName: "amq.direct");
                        Task<BaseResponseMessageContext> task3 = publisher.SendAsync(methodname + ".a1", message3, exchangeName: "amq.direct");
                        Task<BaseResponseMessageContext> task4 = publisher.SendAsync(methodname + ".a1", message4, exchangeName: "amq.direct");
                        Task<BaseResponseMessageContext> task5 = publisher.SendAsync(methodname + ".a1", message5, exchangeName: "amq.direct");
                        Task<BaseResponseMessageContext> task6 = publisher.SendAsync(methodname + ".a1", message6, exchangeName: "amq.direct");
                        Task<BaseResponseMessageContext> task7 = publisher.SendAsync(methodname + ".a1", message7, exchangeName: "amq.direct");
                        Task<BaseResponseMessageContext> task8 = publisher.SendAsync(methodname + ".a1", message8, exchangeName: "amq.direct");
                        Task<BaseResponseMessageContext> task9 = publisher.SendAsync(methodname + ".a1", message9, exchangeName: "amq.direct");
                        Task<BaseResponseMessageContext> task10 = publisher.SendAsync(methodname + ".a1", message10, exchangeName: "amq.direct");

                        DateTime start = DateTime.Now;

                        Task.WaitAll(task1, task2, task3, task4, task5, task6, task7, task8, task9, task10);

                        DateTime end = DateTime.Now;

                        Assert.Equal(message1.Context, task1.Result.Context);
                        Assert.Equal(message2.Context, task2.Result.Context);
                        Assert.Equal(message3.Context, task3.Result.Context);
                        Assert.Equal(message4.Context, task4.Result.Context);
                        Assert.Equal(message5.Context, task5.Result.Context);
                        Assert.Equal(message6.Context, task6.Result.Context);
                        Assert.Equal(message7.Context, task7.Result.Context);
                        Assert.Equal(message8.Context, task8.Result.Context);
                        Assert.Equal(message9.Context, task9.Result.Context);
                        Assert.Equal(message10.Context, task10.Result.Context);
                        Assert.InRange((end - start).Milliseconds, 0, 5000);
                    }
                    
                }
            }
        }
    }
}
