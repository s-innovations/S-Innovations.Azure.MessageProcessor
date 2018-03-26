using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Rest;
using SInnovations.Azure.MessageProcessor.ServiceBus;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace MessageProcessor.CLI
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var ac = new AuthenticationContext("https://login.windows.net/802626c6-0f5c-4293-a8f5-198ecd481fe3", true, null);


            var codeResult = await ac.AcquireDeviceCodeAsync(
                                     resource: "https://management.azure.com/",
                                     clientId: "1950a258-227b-4e31-a9cf-717495945fc2");
            Console.WriteLine(codeResult.Message);

            AuthenticationResult AuthenticationInfo = ac.AcquireTokenByDeviceCodeAsync(codeResult).Result;
            //  Console.WriteLine(AuthenticationInfo.AccessToken);

            var client = new Microsoft.Azure.Management.ServiceBus.ServiceBusManagementClient(new TokenCredentials(AuthenticationInfo.AccessToken));
            client.SubscriptionId = "1626d2da-4051-4674-9d4c-57ce23d967a3";// https://management.azure.com/subscriptions/1626d2da-4051-4674-9d4c-57ce23d967a3/resourceGroups/earthml-core/providers/Microsoft.ServiceBus/namespaces/earthml

            var conn = await client.Namespaces.ListKeysWithHttpMessagesAsync("earthml-core", "earthml", "RootManageSharedAccessKey");
        
            
           

            var options = new ServiceBusMessageProcessorProviderOptions
            {
                ConnectionString = conn.Body.PrimaryConnectionString,
                ResourceGroup = "earthml-core",
                Client = client,
                TopicScaleCount = 2,
                TopicDescription = new TopicDescription("earthml-documents"),
                SubscriptionDescription = new SubscriptionDescription("earthml-documents","sub"),
                CorrelationToQueueMapping = new Dictionary<string, EntityDescription>
                      {
                            { "default",  new QueueDescription("earthml-default") },
                            { "EarthML.Pimetr",  new QueueDescription("earthml-pimetr") },
                            { "EarthML.Identity",  new QueueDescription("earthml-identity") },
                            { "EarthML.Notifications", new TopicDescription("signalr") }
                      },
                //  CorrelationIdProvider = CorrelationIdProvider
            };

            var test = new ServiceBusMessageProcessorProvider(new LoggerFactory(), options);

            test.EnsureTopicsAndQueuesCreatedAsync().Wait(); ;

           
           
        }
    }
}
