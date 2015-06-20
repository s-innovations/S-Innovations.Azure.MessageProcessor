using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.Core.Notifications
{
    public class MessageCompletedNotification : BaseNotificationMessage
    {
        public MessageCompletedNotification(IMessageHandlerResolver resolver) : base(resolver) { }
        public TimeSpan Elapsed { get; set; }
        public TimeSpan ElapsedUntilReceived { get; set; }

        public int WorkingCount { get; set; }
    }
    public class MessageStartedNotification : BaseNotificationMessage
    {
        public MessageStartedNotification(IMessageHandlerResolver resolver) : base(resolver) { }
        public TimeSpan Elapsed { get; set; }


        public int WorkingCount { get; set; }
    }
}
