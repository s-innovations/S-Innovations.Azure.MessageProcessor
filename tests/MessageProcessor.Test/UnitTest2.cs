using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SInnovations.Azure.MessageProcessor.Core;
using SInnovations.Azure.MessageProcessor.Core.Schedules;
using System.Reflection;
using SInnovations.Azure.MessageProcessor.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System.Collections.Generic;
using System.Threading.Tasks;
using SInnovations.Azure.MessageProcessor.Unity;
using Microsoft.Practices.Unity;

namespace SInnovations.Azure.MessageProcessor.Test
{
    [Serializable]
    [MessageSchedule(TimeBetweenSchedules = "PT5M")]
    public class ScheduleMessageTst : BaseMessage
    {
        public string Id { get; set; }
    }

    public class ScheduleMessageTstHandler : IMessageHandler<ScheduleMessageTst>
    {
        public static int counter = 0;
        public async Task HandleAsync(ScheduleMessageTst T)
        {
            //DO Work every 5min
            counter++;
        }
    }


    [TestClass]
    public class UnitTest2
    {
      //  [TestMethod]
        public async Task TestMethod1()
        {
            var attr = typeof(ScheduleMessageTst).GetCustomAttribute<MessageScheduleAttribute>();



            var connectionString = "Endpoint=sb://car2cloudtest.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=C/xWLwrLhqdnKvsNYlc9nUx3Og8lSkIcliSthKXoRys="; int scalecount = 2;
            var options = new ServiceBusMessageProcessorProviderOptions()
            {
                ConnectionString = connectionString,
                TopicScaleCount = scalecount,
                CorrelationIdProvider = (message) =>
                {
                    return (message as ScheduleMessageTst).Id;
                },
                CorrelationToQueueMapping = new Dictionary<string, EntityDescription>{
                    {"test", new QueueDescription("test"){ RequiresDuplicateDetection = true, DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(1) }},
                    {"anothertest",new QueueDescription("another-test"){ RequiresDuplicateDetection = true, DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(1) }}
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

            for (var i = 0; i < 10; i++)
            {

                provider.SendMessageAsync(new ScheduleMessageTst { Id = "test" }).Wait();
            }

            var conatiner = new UnityContainer();
            conatiner.RegisterInstance<IMessageProcessorClientProvider<BrokeredMessage>>(provider);
            conatiner.RegisterType<IMessageHandler<ScheduleMessageTst>, ScheduleMessageTstHandler>();
           

            var listener = new MessageProcessorClient<BrokeredMessage>(new MessageProcessorClientOptions<BrokeredMessage>
            {
                Provider = new ServiceBusMessageProcessorProvider(
               new ServiceBusMessageProcessorProviderOptions
               {
                   ConnectionString = connectionString,
                   QueueDescription = new QueueDescription("test"),
                   MaxConcurrentProcesses = 2,
               }),
                ResolverProvider = ()=> new UnityHandlerResolver(conatiner)

            });
          
            await listener.StartProcessorAsync();

            var datetime = DateTime.UtcNow.AddMinutes(10);
            while (datetime > DateTime.UtcNow)
            {

                await Task.Delay(10000);
            }
            Assert.AreEqual(2, ScheduleMessageTstHandler.counter);
        }
    }
}
