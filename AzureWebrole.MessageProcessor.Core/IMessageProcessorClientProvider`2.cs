using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.Core
{
    public interface IMessageProcessorClientProvider<TOptions, MessageType> : IDisposable where TOptions : IMessageProcessorProviderOptions<MessageType>
    {
        TOptions Options { get; }
        T FromMessage<T>(MessageType m) where T : BaseMessage;
       
    }
}
