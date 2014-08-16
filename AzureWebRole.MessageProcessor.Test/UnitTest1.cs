using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.IO;
using AzureWebRole.MessageProcessor.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.Collections.Generic;
using System.Runtime.Serialization;
using AzureWebRole.MessageProcessor.Core;

namespace AzureWebRole.MessageProcessor.Test
{

    [Serializable]
    public class TestMessage : BaseMessage
    {
        public string Id {get;set;}


        public AnotherMeessage PostMessage { get; set; }
    }
    
    [Serializable]
    [KnownType(typeof(Test2Message))]
    public class AnotherMeessage : TestMessage
    {
        public string Test { get; set; }
    }

    [Serializable]
    
    public class Test2Message : AnotherMeessage
    {
        public string Test2 { get; set; }
    }
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {

#if DEBUG


            var connectionString = ""; int scalecount = 10;
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
                    {"test",connectionString},
                    {"anothertest",connectionString}
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
                        {"test",connectionString},
                        {"anothertest",connectionString}
                    },
                    TopicDescription = new TopicDescription("ourTopics"),
                });
            for (var i = 0; i < 10; i++)
            {
                anotherProvider.SendMessageAsync(new TestMessage { Id = "test", PostMessage = new Test2Message() { Test = "asd", Test2="adsa" } }).Wait();
                anotherProvider.SendMessageAsync(new TestMessage { Id = "anothertest" }).Wait();
                anotherProvider.SendMessageAsync(new TestMessage { Id = "test" }).Wait();
            }

#endif

        }
    }
}
