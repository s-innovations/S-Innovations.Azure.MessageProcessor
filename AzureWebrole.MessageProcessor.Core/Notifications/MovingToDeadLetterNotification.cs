using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.Core.Notifications
{
    public class MovingToDeadLetterNotification : BaseNotificationMessage
    {

        public MovingToDeadLetterNotification(IMessageHandlerResolver resolver) : base(resolver) { }
        public bool Cancel { get; set; }

    }
}
