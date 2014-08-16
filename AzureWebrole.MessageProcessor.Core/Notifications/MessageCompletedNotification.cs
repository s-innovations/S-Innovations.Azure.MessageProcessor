using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.Core.Notifications
{
    public class MessageCompletedNotification : BaseNotificationMessage
    {
        public MessageCompletedNotification(IMessageHandlerResolver resolver) : base(resolver) { }
        public TimeSpan Elapsed { get; set; }
    }
}
