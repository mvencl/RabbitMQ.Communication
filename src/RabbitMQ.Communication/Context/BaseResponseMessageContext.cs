using System;

namespace RabbitMQ.Communication.Context
{
    public class BaseResponseMessageContext : BaseMessageContext
    {
        public bool IsError { get { return Exception != null; } }

        public Exception Exception { get; set; } = null;
               

    }
}
