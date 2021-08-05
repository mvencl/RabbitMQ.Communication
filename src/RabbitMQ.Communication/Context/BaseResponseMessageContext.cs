using Newtonsoft.Json;
using System;

namespace RabbitMQ.Communication.Context
{
    public class BaseResponseMessageContext : BaseMessageContext
    {
        public bool IsError { get { return Exception != null; } }

        [JsonProperty(TypeNameHandling = TypeNameHandling.All)]
        public Exception Exception { get; set; } = null;
               

    }
}
