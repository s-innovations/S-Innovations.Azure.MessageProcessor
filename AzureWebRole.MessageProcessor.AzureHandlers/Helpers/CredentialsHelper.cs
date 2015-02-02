using AzureWebRole.MessageProcessor.AzureHandlers.Messages;
using Microsoft.Azure;
using Microsoft.WindowsAzure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.AzureHandlers.Helpers
{
    public static class CredentialsHelper
    {
        public async static Task<SubscriptionCloudCredentials> GetCredentials(AzureSubscriptionBaseMessage message, ICertificateLocator certificates = null)
        {
            SubscriptionCloudCredentials cred =
               !string.IsNullOrWhiteSpace(message.AzureSubscriptionCertificateThumbprint) ?
                 new CertificateCloudCredentials(message.AzureSubscriptionId,
                   await certificates.GetCertificateAsync(message.AzureSubscriptionCertificateThumbprint)) as SubscriptionCloudCredentials :
               (!string.IsNullOrWhiteSpace(message.AzureSubscriptionToken) ?
                   new TokenCloudCredentials(message.AzureSubscriptionId) : null);

            if (cred == null)
                throw new Exception("No Credentials Given");

            return cred;
        }
    }
}
