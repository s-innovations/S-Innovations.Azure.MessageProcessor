using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using AzureWebRole.MessageProcessor.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.Collections.Generic;
using AzureWebrole.MessageProcessor.Core;

namespace AzureWebRole.MessageProcessor.Test
{

    public class TestMessage : BaseMessage
    {
        public string Id {get;set;}
    }
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {

#if DEBUG


            var connectionString = "Endpoint=sb://scaletest3.servicebus.windows.net/;SharedSecretIssuer=owner;SharedSecretValue=oVC0tZZ4pULz7r9Cq5/WpMqqcNUj/v6x8zOTshGCyT0=";
            int scalecount = 10;
            //Setting up everything (run Once)
            var options = new ServiceBusMessageProcessorProviderOptions()
            {
                ConnectionString = connectionString,
                TopicScaleCount = scalecount,
                CorrelationToQueueMapping = new Dictionary<string, EntityDescription>{
                    {"test", new QueueDescription("test") },
                    {"anothertest",new QueueDescription("another-test")}
                  },
                //Subscription Description is only used for properties passing to the created subscriptions. here sub is prefixname of forward subscriptions
                SubscriptionDescription = new SubscriptionDescription("blablanotUsed", "sub"),
                //Topic is used for properties and topic nameprefix, this case ourtopics.
                TopicDescription = new TopicDescription("ourTopics"),
                ConnectionStringProvider = new Dictionary<string, string>
                {
                    {"test","Endpoint=sb://scaletest1.servicebus.windows.net/;SharedSecretIssuer=owner;SharedSecretValue=WA3Q4aORYuDtEywWHIPY6I2lhZ/5zCgZy+DhdyPQEaQ="},
                    {"anothertest","Endpoint=sb://scaletest2.servicebus.windows.net/;SharedSecretIssuer=owner;SharedSecretValue=bL8Jt58rlNUO1z9Lw3fT04tC+YiuSo8jFMPavNrI4EY="}
                }
            };

            
            var provider = new ServiceBusMessageProcessorProvider(options);
            provider.EnsureTopicsAndQueuesCreatedAsync().Wait();

            //This is what is needed to post messages to the system.

            var anotherProvider = new ServiceBusMessageProcessorProvider(
                new ServiceBusMessageProcessorProviderOptions
                {
                    ConnectionString = connectionString,
                    TopicScaleCount = scalecount,
                    CorrelationIdProvider = (message) =>
                    {
                        return (message as TestMessage).Id;
                    },
                    ConnectionStringProvider = new Dictionary<string, string>
                    {
                        {"test","Endpoint=sb://scaletest1.servicebus.windows.net/;SharedSecretIssuer=owner;SharedSecretValue=WA3Q4aORYuDtEywWHIPY6I2lhZ/5zCgZy+DhdyPQEaQ="},
                        {"anothertest","Endpoint=sb://scaletest2.servicebus.windows.net/;SharedSecretIssuer=owner;SharedSecretValue=bL8Jt58rlNUO1z9Lw3fT04tC+YiuSo8jFMPavNrI4EY="}
                    },
                    TopicDescription = new TopicDescription("ourTopics"),
                });
            for (var i = 0; i < 10; i++)
            {
                anotherProvider.SendMessageAsync(new TestMessage { Id = "test" }).Wait();
                anotherProvider.SendMessageAsync(new TestMessage { Id = "anothertest" }).Wait();
                anotherProvider.SendMessageAsync(new TestMessage { Id = "test" }).Wait();
            }

#endif

        }
    }
}
