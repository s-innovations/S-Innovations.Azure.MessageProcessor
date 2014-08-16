using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.Core
{
    public interface IMessageProcessorProviderOptions<MessageType>
    {
        int MaxMessageRetries { get; }
        TimeSpan? AutoRenewLockTime { get; }
    }
}
