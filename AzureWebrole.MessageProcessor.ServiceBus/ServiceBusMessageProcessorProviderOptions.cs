using AzureWebrole.MessageProcessor.Core;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.ServiceBus
{

    public class ServiceBusMessageProcessorProviderOptions : IMessageProcessorProviderOptions<BrokeredMessage>
    {

        public string ConnectionString { get; set; }
        public TopicDescription TopicDescription { get; set; }
        public SubscriptionDescription SubscriptionDescription { get; set; }
        public QueueDescription QueueDescription { get; set; }

        public int MaxConcurrentProcesses { get; set; }

        public int MaxMessageRetries { get; set; }
      
    }
}
