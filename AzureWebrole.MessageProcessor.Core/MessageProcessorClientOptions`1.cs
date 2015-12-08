using SInnovations.Azure.MessageProcessor.Core.Notifications;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.Core
{
    public class MessageProcessorClientOptions<MessageType>
    {
        public MessageProcessorClientOptions()
        {
            Notifications = new DefaultNotifications();
        }
        public IMessageProcessorNotifications Notifications { get; set; }
        public IMessageProcessorClientProvider<MessageType> Provider { get; set; }

        public Func<IMessageHandlerResolver> ResolverProvider { get; set; }

        public TimeSpan? IdleTimeCheckInterval { get; set; }

        public TimeSpan? AutoRenewLockTimerDuration { get; set; }

        public TimeSpan? HandlerTimeOut { get; set; }

        public Func<BaseMessage, TimeSpan?> MessageBasedTimeOutProvider { get; set; }

     
    }
}
