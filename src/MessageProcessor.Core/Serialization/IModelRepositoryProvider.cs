using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.Core.Serialization
{
 
    public interface IModelRepositoryProvider
    {
        IModelRepository GetRepository();
       
    }
    public interface IModelRepository
    {
        Task SaveModelAsync(IModelBasedMessage message);
        Task GetModelAsync(IModelBasedMessage message);
    }
}
