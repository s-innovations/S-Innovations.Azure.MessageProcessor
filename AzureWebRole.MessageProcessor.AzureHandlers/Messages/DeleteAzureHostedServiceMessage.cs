using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.AzureHandlers.Messages
{
    public class DeleteAzureHostedServiceMessage : AzureSubscriptionBaseMessage
    {
      
        public string DeploymentId { get; set; }

        public string[] RoleInstanceId { get; set; }
    }
}
