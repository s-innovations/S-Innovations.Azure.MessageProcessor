using Microsoft.ServiceBus.Messaging;
using SInnovations.Azure.MessageProcessor.Core;

namespace SInnovations.Azure.MessageProcessor.ServiceBus
{
    public interface ServiceBusMessageProcessorClientProvider : IMessageProcessorClientProvider<BrokeredMessage>
    {

    }
}
