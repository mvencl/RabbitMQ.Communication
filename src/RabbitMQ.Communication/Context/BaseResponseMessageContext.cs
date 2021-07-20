using RabbitMQ.Communication.Contracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace RabbitMQ.Communication.Context
{
    public class BaseResponseMessageContext : BaseMessageContext
    {
        public bool IsError { get { return Exception != null; } }

        public Exception Exception { get; set; } = null;
               

    }
}
