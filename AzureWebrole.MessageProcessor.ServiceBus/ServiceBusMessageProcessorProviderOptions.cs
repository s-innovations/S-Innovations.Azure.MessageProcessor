using AzureWebrole.MessageProcessor.Core;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.ServiceBus
{

    public class ServiceBusMessageProcessorProviderOptions : IMessageProcessorProviderOptions<BrokeredMessage>
    {


        public string ConnectionString { get; set; }

       

        /// <summary>
        /// ConnectionStringProvider such messages will be pushed to respectivly to the given servicebus by its connectionstring.
        /// 
        /// Good for Dev/Test where you can plug in different servicebus instances based on conditions.
        /// </summary>
        public IDictionary<string, string> ConnectionStringProvider { get; set; }


        /// <summary>
        /// The topic to push or listhen to.
        /// 
        /// If pushing message and a TopicScaleCount is provided then it will create 
        /// TopicScaleCount topics with TopicDescription.Path + counter as name. Then based on
        /// CorrelationToQueueMapping there are created subscriptions on each of them that forward to 
        /// the mapping target. This provide high scaleability when pushing message onto the bus. 
        /// 
        /// ServiceBus provides filtering internal based on correlation id which give higher thoughtput 
        /// using more topics. 
        /// 
        /// Not Implemented yet, but if no Mapping is provided all of these should then just forward to the 
        /// TopicDescription.Path or alternative have a property for MultiListening that would ensure no forwarding
        /// and the listening solution would have to create webroles or listeners for each topic.
        /// </summary>
        public TopicDescription TopicDescription { get; set; }



        public SubscriptionDescription SubscriptionDescription { get; set; }
        public QueueDescription QueueDescription { get; set; }

        public int MaxConcurrentProcesses { get; set; }

        public int MaxMessageRetries { get; set; }

        /// <summary>
        /// The CorrelationId Provider. A function that returns a Correlation Id for messesages 
        /// such service bus can filter based on these. 
        /// </summary>
        public Func<BaseMessage, string> CorrelationIdProvider { get; set; }

        /// <summary>
        /// Set the Scale Out Count. Meaning that it will create X subscriptions on topics and forward them to common Queue.
        /// This is good if the system is pushing many messages onto the bus.
        /// </summary>
        public int? TopicScaleCount { get; set; }

        /// <summary>
        /// This can be used to set a correlation to Queue/Topic Mapping.
        /// 
        /// <example>
        ///         CorrelationToQueueMapping = new Dictionary<string, EntityDescription>
        ///            {
        ///                { Constants.ServiceBus.AlgorithmA1QueueName, new QueueDescription(Constants.ServiceBus.AlgorithmA1QueueName)},//$0,074/t 
        ///                { Constants.ServiceBus.AlgorithmA2QueueName, new QueueDescription(Constants.ServiceBus.AlgorithmA2QueueName)},//$0,148/t 
        ///                { Constants.ServiceBus.AlgorithmA3QueueName, new QueueDescription(Constants.ServiceBus.AlgorithmA3QueueName)},//$0,296/t 
        ///                { Constants.ServiceBus.AlgorithmA5QueueName, new QueueDescription(Constants.ServiceBus.AlgorithmA5QueueName)},//$0,33/t 
        ///                { Constants.ServiceBus.AlgorithmA4QueueName, new QueueDescription(Constants.ServiceBus.AlgorithmA4QueueName)},//$0,592/t 
        ///                { Constants.ServiceBus.AlgorithmA6QueueName, new QueueDescription(Constants.ServiceBus.AlgorithmA6QueueName)},//$0,66/t 
        ///                { Constants.ServiceBus.DataQueueName, new QueueDescription(Constants.ServiceBus.DataQueueName)},
        ///                { Constants.ServiceBus.DefaultQueueName, new QueueDescription( Constants.ServiceBus.DefaultQueueName)},
        ///                { Constants.ServiceBus.SignalRTopicName, new TopicDescription(Constants.ServiceBus.SignalRTopicName)}
        ///           },
        /// </example>
        /// </summary>
        public IDictionary<string, EntityDescription> CorrelationToQueueMapping { get; set; }
    }
}
