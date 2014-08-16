using AzureWebRole.MessageProcessor.Core.Notifications;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.Core
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
    }
}
