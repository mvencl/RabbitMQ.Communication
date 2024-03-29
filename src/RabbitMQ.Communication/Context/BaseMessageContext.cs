﻿using Newtonsoft.Json;
using RabbitMQ.Communication.Contracts;

namespace RabbitMQ.Communication.Context
{
    public class BaseMessageContext : IMessageContext
    {
        public BaseMessageContext() { }
        public BaseMessageContext(string context) : base() { Context = context; }

        public string Context { get; set; }

        public T GetContext<T>()
        {
            return JsonConvert.DeserializeObject<T>(Context);
        }

        public void SetContext<T>(T context)
        {
            Context = JsonConvert.SerializeObject(context);
        }
    }
}
