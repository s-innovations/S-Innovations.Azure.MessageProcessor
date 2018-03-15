using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using SInnovations.Azure.MessageProcessor.Core;

namespace SInnovations.Azure.MessageProcessor.ServiceBus
{
    public interface ServiceBusMessageProcessorClientProvider : IMessageProcessorClientProvider<Message >
    {

    }
}
