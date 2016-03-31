using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.Core
{
    public interface IMessageHandler<MessageType> where MessageType : BaseMessage
    {
        Task HandleAsync(MessageType T);
    }
}
