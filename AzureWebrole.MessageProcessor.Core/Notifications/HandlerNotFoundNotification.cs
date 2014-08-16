using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.Core.Notifications
{
    public class HandlerNotFoundNotification : BaseNotificationMessage
    {
        public HandlerNotFoundNotification(IMessageHandlerResolver resolver) : base(resolver) { }
        public Type HandlerType { get; set; }
        public Object Handler { get; set; }
    }
}
