using AzureWebrole.MessageProcessor.Core;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Linq;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.ServiceBus
{

    public class ServiceBusMessageProcessorProvider : IMessageProcessorClientProvider<BrokeredMessage>
    {

        private readonly ServiceBusMessageProcessorProviderOptions options;

        private MessageClientEntity Client;
        private Lazy<TopicClient> LazyTopicClient;
        private Lazy<QueueClient> LazyQueueClient;

        public bool SupportTopic { get { return options.TopicDescription != null; } }
        public bool SupportQueue { get { return options.QueueDescription != null; } }
        public bool SupportSubscription { get { return options.SubscriptionDescription != null; } }

        public ServiceBusMessageProcessorProvider(ServiceBusMessageProcessorProviderOptions options)
        {
            this.options = options;

            if (SupportTopic)
                LazyTopicClient = new Lazy<TopicClient>(() =>
                {

                    var namespaceManager =
                        NamespaceManager.CreateFromConnectionString(this.options.ConnectionString);

                    if (!namespaceManager.TopicExists(this.options.TopicDescription.Path))
                    {
                        namespaceManager.CreateTopic(this.options.TopicDescription);
                    }

                    return TopicClient.CreateFromConnectionString(this.options.ConnectionString, options.TopicDescription.Path);
                });
            if (SupportQueue)
                LazyQueueClient = new Lazy<QueueClient>(() =>
                {
                    var namespaceManager =
                       NamespaceManager.CreateFromConnectionString(this.options.ConnectionString);

                    if (!namespaceManager.QueueExists(this.options.QueueDescription.Path))
                    {
                        namespaceManager.CreateQueue(this.options.QueueDescription);
                    }

                    return QueueClient.CreateFromConnectionString(this.options.ConnectionString, options.TopicDescription.Path);

                });
        }
        public void StartListening(Func<BrokeredMessage, Task> OnMessageAsync)
        {

            string connectionString = this.options.ConnectionString;

            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(connectionString);
            var tasks = new List<Task>();

            if (SupportTopic && !namespaceManager.TopicExists(this.options.TopicDescription.Path))
            {
                tasks.Add(namespaceManager.CreateTopicAsync(this.options.TopicDescription));
            }

            if (SupportSubscription && !namespaceManager.SubscriptionExists(this.options.TopicDescription.Path, this.options.SubscriptionDescription.Name))
            {
                tasks.Add(namespaceManager.CreateSubscriptionAsync(this.options.SubscriptionDescription));
            }

            Task.WaitAll(tasks.ToArray());


            var messageOptions = new OnMessageOptions { MaxConcurrentCalls = this.options.MaxConcurrentProcesses, AutoComplete = false };
            messageOptions.ExceptionReceived += options_ExceptionReceived;

            if (SupportSubscription && SupportTopic)
            {
                var client = SubscriptionClient.CreateFromConnectionString
                  (connectionString, this.options.TopicDescription.Path, this.options.SubscriptionDescription.Name);

                client.OnMessageAsync(OnMessageAsync, messageOptions);
                Client = client;
            }
            else if (SupportQueue)
            {
                var client = LazyQueueClient.Value;
                client.OnMessageAsync(OnMessageAsync, messageOptions);
                Client = client;
            }
            else
            {
                throw new Exception("No listening client was started. Configure eitehr a queue or subscription client to start");
            }

        }

        public T FromMessage<T>(BrokeredMessage m) where T : BaseMessage
        {
            var messageBodyType =
                      Type.GetType(m.Properties["messageType"].ToString());
            if (messageBodyType == null)
            {
                //Should never get here as a messagebodytype should
                //always be set BEFORE putting the message on the queue
                Trace.TraceError("Message does not have a messagebodytype" +
                  " specified, message {0}", m.MessageId);
                m.DeadLetter();
            }
            MethodInfo method = typeof(BrokeredMessage).GetMethod("GetBody", new Type[] { });
            MethodInfo generic = method.MakeGenericMethod(messageBodyType);
            var messageBody = generic.Invoke(m, null);
            return messageBody as T;
        }
        public BrokeredMessage ToMessage<T>(T message) where T : BaseMessage
        {
            var brokeredMessage = new BrokeredMessage(message);
            brokeredMessage.Properties["messageType"] = message.GetType().AssemblyQualifiedName;
            return brokeredMessage;
        }

        private void options_ExceptionReceived(object sender, ExceptionReceivedEventArgs e)
        {
            Trace.TraceError("{0} {1}", e.Exception, e.Exception.InnerException);
        }

        public void Dispose()
        {
            if (Client != null) ;
            Client.Close();
        }




        public IMessageProcessorProviderOptions<BrokeredMessage> Options
        {
            get { return options; }
        }


        public Task SendMessageAsync(BrokeredMessage message)
        {
            return LazyTopicClient.Value.SendAsync(message);
        }
        public Task SendMessagesAsync(IEnumerable<BrokeredMessage> messages)
        {
            return LazyTopicClient.Value.SendBatchAsync(messages);
        }

        public Task SendMessageAsync<T>(T message) where T : BaseMessage
        {
            var brokeredMessage = new BrokeredMessage(message);
            brokeredMessage.Properties["messageType"] = message.GetType().AssemblyQualifiedName;
            return SendMessageAsync(brokeredMessage);
        }
        public Task SendMessagesAsync<T>(IEnumerable<T> messages) where T : BaseMessage
        {
            return SendMessagesAsync(messages.Select(ToMessage));
        }

        public int GetDeliveryCount(BrokeredMessage message)
        {
            return message.DeliveryCount;
        }


        public Task CompleteMessageAsync(BrokeredMessage message)
        {
            return message.CompleteAsync();
        }
    }
}
