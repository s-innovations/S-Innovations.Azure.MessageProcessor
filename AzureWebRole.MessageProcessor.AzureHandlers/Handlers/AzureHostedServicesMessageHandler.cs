﻿using AzureWebRole.MessageProcessor.AzureHandlers.Messages;
using AzureWebRole.MessageProcessor.Core;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Management.Compute.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace AzureWebRole.MessageProcessor.AzureHandlers.Handlers
{
    public class AzureHostedServicesMessageHandler : IMessageHandler<DeployAzureHostedServiceMessage>
    {
        private readonly ICertificateLocator certificates;
        public AzureHostedServicesMessageHandler(ICertificateLocator certificates)
        {
            this.certificates = certificates;
        }
        public async Task HandleAsync(DeployAzureHostedServiceMessage message)
        {
          
            SubscriptionCloudCredentials cred = 
                !string.IsNullOrWhiteSpace(message.AzureSubscriptionCertificateThumbprint) ? 
                  new CertificateCloudCredentials(message.AzureSubscriptionId,
                    await certificates.GetCertificateAsync(message.AzureSubscriptionCertificateThumbprint)) as SubscriptionCloudCredentials :
                (!string.IsNullOrWhiteSpace(message.AzureSubscriptionToken) ? 
                    new TokenCloudCredentials(message.AzureSubscriptionId) : null);

            if (cred == null)
                throw new Exception("No Credentials Given");

            using (var management = CloudContext.Clients.CreateComputeManagementClient(cred))
            {
                var services = await management.HostedServices.ListAsync();

                if (!services.Any(s => s.ServiceName.Equals(message.HostedServiceName)))
                {
                    await CreateHostedService(message, management);
                }


                var service = await management.HostedServices.GetDetailedAsync(message.HostedServiceName);

                if (service.Deployments.Any() && !message.CanUpgrade)
                {
                    Trace.TraceInformation("The service '{0}' already had a deployment running and message was not allowed to upgrade. Skipped deployment.", message.HostedServiceName);
                    return;
                }

                
                
                XDocument document = null;
                if (!string.IsNullOrWhiteSpace(message.PackageConfigurationUri))
                    document = XDocument.Load(message.PackageConfigurationUri);
                else if (message.PackageConfiguration != null)
                    document = XDocument.Parse(message.PackageConfiguration);

                if (document == null)
                    throw new Exception("The message did not have a valid PackageConfiguration Setting, specify either uri or xml");

                await HandleServiceCertificates(message, management, document);
                OperationStatusResponse response = null;
                if (service.Deployments.Any())
                {
                    response = await HandleUpgradeAsync(message, management, document, response);
                }
                else
                {

                    response = await HandleDeployAsync(message, management, document, response);
                }

                bool running = false;
                var starttime = DateTime.UtcNow;
                while (!running)
                {
                    var production = await management.Deployments.GetBySlotAsync(message.HostedServiceName, DeploymentSlot.Production);
                    if (production.RoleInstances.Any())
                    {
                        running = production.RoleInstances.All(instance => instance.InstanceStatus == "ReadyRole");
                    }
                    var waittime = (DateTime.UtcNow - starttime);

                    if(waittime.TotalMinutes > message.DeploymentTimeoutInMinutes )
                    {
                        throw new Exception("Deployment Timeout: " + string.Join(", ",production.RoleInstances.Select(i =>i.InstanceStatus)));
                    }
                    if (!running)
                        await Task.Delay(5000);
                }

            }
        }

        private static async Task<OperationStatusResponse> HandleDeployAsync(DeployAzureHostedServiceMessage message, Microsoft.WindowsAzure.Management.Compute.ComputeManagementClient management, XDocument document, OperationStatusResponse response)
        {
            var deployParameter = new DeploymentCreateParameters
            {
                StartDeployment = true,
                PackageUri = new Uri(message.HostedServicePackageSasUri),
                Name = message.DeploymentName,
                Label = message.DeploymentLabel ?? message.DeploymentName,
                Configuration = document.ToString(),

            };
            if (!string.IsNullOrWhiteSpace(message.DeploymentJsonExtendedProperties))
                deployParameter.ExtendedProperties = JsonConvert.DeserializeObject<Dictionary<string, string>>(message.DeploymentJsonExtendedProperties);


            response = await management.Deployments.CreateAsync(message.HostedServiceName,
                   DeploymentSlot.Production,
                   deployParameter);
            return response;
        }

        private static async Task<OperationStatusResponse> HandleUpgradeAsync(DeployAzureHostedServiceMessage message, Microsoft.WindowsAzure.Management.Compute.ComputeManagementClient management, XDocument document, OperationStatusResponse response)
        {
            var upgradeParameters = new DeploymentUpgradeParameters
            {
                Configuration = document.ToString(),
                PackageUri = new Uri(message.HostedServicePackageSasUri),
                Label = message.DeploymentLabel ?? message.DeploymentName,
                Force = true,
                Mode = DeploymentUpgradeMode.Auto
            };

            response = await management.Deployments.UpgradeBySlotAsync(message.HostedServiceName, DeploymentSlot.Production, upgradeParameters);
            return response;
        }

        private async Task HandleServiceCertificates(DeployAzureHostedServiceMessage message, Microsoft.WindowsAzure.Management.Compute.ComputeManagementClient management, XDocument document)
        {
            var certificatesNeeded = document.Descendants()
                .Where(e => e.Name.LocalName.Equals("Certificate"))
                .Select(e => (string)e.Attribute("thumbprint")).ToArray();

            if (!certificatesNeeded.Any())
                return;

            var existingCertificates = await management.ServiceCertificates.ListAsync(message.HostedServiceName);

            foreach (var certificateThumbprint in certificatesNeeded)
            {

                if (existingCertificates.Certificates.Any(
                        c => string.Equals(c.Thumbprint, certificateThumbprint, StringComparison.OrdinalIgnoreCase)))
                {
                    continue;
                }

                ServiceCertificateCreateParameters certificate = await certificates.GetServiceCertificateAsync(certificateThumbprint);


                await management.ServiceCertificates.CreateAsync(message.HostedServiceName, certificate);

            }
        }

        private static async Task CreateHostedService(DeployAzureHostedServiceMessage message, Microsoft.WindowsAzure.Management.Compute.ComputeManagementClient management)
        {
            if (!message.CanCreate)
            {
                throw new Exception("Service not created, message.CanCreate was false.");
            }
            var avalible = await management.HostedServices.CheckNameAvailabilityAsync(message.HostedServiceName);
            if (!avalible.IsAvailable)
            {
                string exceptionMessage = "Service cloud not be created: " + avalible.Reason;

                throw new Exception(exceptionMessage);
            }

           var parameters =new HostedServiceCreateParameters
            {
                AffinityGroup = message.AffinityGroup,
                Description = "Automatic Created Cloud Service",
                Label = message.HostedServiceName,
                ServiceName = message.HostedServiceName,
              
            };
           if (!string.IsNullOrWhiteSpace(message.ServiceJsonExtendedProperties))
               parameters.ExtendedProperties = JsonConvert.DeserializeObject<Dictionary<string, string>>(message.ServiceJsonExtendedProperties);
             
            var operation = await management.HostedServices.CreateAsync(parameters);

        }
    }
}