﻿using SInnovations.Azure.MessageProcessor.AzureHandlers.Helpers;
using SInnovations.Azure.MessageProcessor.AzureHandlers.Messages;
using SInnovations.Azure.MessageProcessor.Core;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Management.Compute;
using Microsoft.WindowsAzure.Management.Compute.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.AzureHandlers.Handlers
{
    public class DeleteAzureHostedServicesDeploymentMessageHandler : IMessageHandler<DeleteAzureHostedServiceMessage>
    {
         private readonly ICertificateLocator certificates;
         public DeleteAzureHostedServicesDeploymentMessageHandler(ICertificateLocator certificates)
        {
            this.certificates = certificates;
        }
        public async Task HandleAsync(DeleteAzureHostedServiceMessage message)
        {
            var cred = await CredentialsHelper.GetCredentials(message,certificates);

            using (var management = new ComputeManagementClient(cred))
            {
                var services = await management.HostedServices.ListAsync();
                Trace.TraceInformation("{0} Hosted Services Found. Looking for {1} in {2}"
                ,services.Count(), JsonConvert.SerializeObject(message), JsonConvert.SerializeObject(services.Select(s=>new {s.ServiceName}).ToArray()));
                
                foreach(var service in services)
                {
                    var details = await management.HostedServices.GetDetailedAsync(service.ServiceName);
                    
                    if(details.Deployments.Any(d=>d.PrivateId == message.DeploymentId))
                    {
                        var deployment = details.Deployments.First(d => d.PrivateId == message.DeploymentId);
                        if (deployment.RoleInstances.Count == 1 && 
                            deployment.RoleInstances.First().InstanceName == message.RoleInstanceId.First())
                        {
                            Trace.TraceInformation("Killing last instance: {0} ", service.ServiceName);
                          
                            await management.Deployments.DeleteBySlotAsync(service.ServiceName, deployment.DeploymentSlot);

                        }else{

                            Trace.TraceInformation("Killing instance:{0}", service.ServiceName);
                            await management.Deployments.DeleteRoleInstanceByDeploymentNameAsync(
                            service.ServiceName,
                            deployment.Name,
                            new DeploymentDeleteRoleInstanceParameters 
                            {
                                Name = new List<string>(message.RoleInstanceId)
                            });
                        }
                    }
                }
            }
        }
    }
}
