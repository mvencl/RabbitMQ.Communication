using RabbitMQ.Communication.Contracts;

namespace RabbitMQ.Communication.Tests
{
    public class BaseStringMessageContext2 : IMessageContext
    {
        public string Context { get; set; }
        public string CorrelationID { get; set; }
    }
}
