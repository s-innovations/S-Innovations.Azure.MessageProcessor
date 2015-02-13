using Microsoft.WindowsAzure.Management.Compute.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.AzureHandlers
{
    public interface ICertificateLocator
    {
        Task<X509Certificate2> GetCertificateAsync(string thumbprint);

        Task<ServiceCertificateCreateParameters> GetServiceCertificateAsync(string certificateThumbprint);
    }
}
