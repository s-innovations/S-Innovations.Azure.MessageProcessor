using SInnovations.Azure.MessageProcessor.Core.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.Core
{
    public interface IMessageProcessorProviderOptions<MessageType>
    {
        int MaxMessageRetries { get; }
        TimeSpan? AutoRenewLockTime { get; }

        IModelRepositoryProvider RepositoryProvider { get; }
    }
}
