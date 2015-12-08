using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.Core.Notifications
{
    public class MessageStartedNotification : BaseNotificationMessage
    {
        public MessageStartedNotification(IMessageHandlerResolver resolver) : base(resolver) { }
        public TimeSpan Elapsed { get; set; }
        public int WorkingCount { get; set; }
    }
}
