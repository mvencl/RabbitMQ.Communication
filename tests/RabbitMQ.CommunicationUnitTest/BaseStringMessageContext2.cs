using RabbitMQ.Communication.Contracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace RabbitMQ.Communication.Tests
{
    public class BaseStringMessageContext2 : IMessageContext
    {
        public string Context { get; set; }
        public string CorrelationID { get; set; }
    }
}
