using AzureWebRole.MessageProcessor.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.AzureHandlers.Messages
{
    public class DeployAzureHostedServiceMessage : AzureSubscriptionBaseMessage
    {
        public DeployAzureHostedServiceMessage()
        {
            CanCreate = false;
            CanUpgrade = false;
            DeploymentTimeoutInMinutes = 30;
        }

        public string HostedServicePackageSasUri{ get; set; }
        public string PackageConfigurationUri { get; set; }
        public string PackageConfiguration { get; set; }
        public string HostedServiceName { get; set; }
        public string AffinityGroup { get; set; }

        public string ServiceJsonExtendedProperties { get; set; }
        public string DeploymentJsonExtendedProperties { get; set; }

        public bool CanCreate { get; set; }
  
        public bool CanUpgrade { get; set; }

        public string DeploymentName { get; set; }
        public string DeploymentLabel { get; set; }

        public int DeploymentTimeoutInMinutes { get; set; }

        public string Key { get; set; }
    }
}
