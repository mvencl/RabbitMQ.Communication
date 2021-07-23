using Newtonsoft.Json;
using RabbitMQ.Communication.Contracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Communication.Context
{
    public class BaseMessageContext : IMessageContext
    {
        public string Context { get; set; }

        public string CorrelationID { get; set; } = Extension.RabbitMQExtension.GetCorrelationId();

        public T GetContext<T>()
        {
            return JsonConvert.DeserializeObject<T>(Context);
        }

        public void SetContext<T>(T context)
        {
            Context = JsonConvert.SerializeObject(context);
        }

        public virtual BaseResponseMessageContext GenerateResponse(string context = null)
        {
            return new BaseResponseMessageContext
            {
                CorrelationID = this.CorrelationID,
                Context = context
            };
        }
    }
}
