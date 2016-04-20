using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.Core
{
    public interface IMessageProcessorClientProvider<TOptions, MessageType> : IDisposable where TOptions : IMessageProcessorProviderOptions<MessageType>
    {
        TOptions Options { get; }
        Task<BaseMessage> FromMessageAsync(MessageType m);
       
    }
}
