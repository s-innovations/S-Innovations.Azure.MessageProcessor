using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.Core.Notifications
{
    public class BaseNotificationMessage
    {
        public BaseNotificationMessage(IMessageHandlerResolver resolver)
        {
            Resolver = resolver;
        }
        public BaseMessage Message { get; set; }
        public IMessageHandlerResolver Resolver { get; set; }

    }
}
