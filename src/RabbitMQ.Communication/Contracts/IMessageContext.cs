using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMQ.Communication.Contracts
{
    public interface IMessageContext
    {
        string Context { get; set; }
                
        string CorrelationID { get; set; }
    }
}
