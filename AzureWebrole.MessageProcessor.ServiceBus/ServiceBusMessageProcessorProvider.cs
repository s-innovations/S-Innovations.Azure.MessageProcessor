using AzureWebrole.MessageProcessor.Core;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebrole.MessageProcessor.ServiceBus
{
    public class ServiceBusMessageProcessorProviderOptions : MessageProcessorProviderOptions<BrokeredMessage>
    {
        public string ConnectionString { get; set; }
        public TopicDescription TopicDescription { get; set; }
        public SubscriptionDescription SubscriptionDescription { get; set; }
        public OnMessageOptions MessageOptions { get; set; }
        //public Func<BrokeredMessage, Task> OnMessage { get; set; }
        public int MaxMessageRetries
        {
            get;
            set;
        }
    }
    public class ServiceBusMessageProcessorProvider : MessageProcessorClientProvider<BrokeredMessage>
    {

        private readonly ServiceBusMessageProcessorProviderOptions options;

        private SubscriptionClient SubscriptionClient;
        private Lazy<TopicClient> LazyTopicClient;
        public ServiceBusMessageProcessorProvider(ServiceBusMessageProcessorProviderOptions options)
        {
            this.options = options;
            LazyTopicClient = new Lazy<TopicClient>(() =>
            {

                string connectionString = this.options.ConnectionString;

                var namespaceManager =
                    NamespaceManager.CreateFromConnectionString(connectionString);

                if (!namespaceManager.TopicExists(this.options.TopicDescription.Path))
                {
                    namespaceManager.CreateTopic(this.options.TopicDescription);
                }
                //if (!namespaceManager.SubscriptionExists(this.options.TopicDescription.Path, this.options.SubscriptionDescription.Name))
                //{
                //    namespaceManager.CreateSubscription(this.options.SubscriptionDescription);
                //}

                return TopicClient.CreateFromConnectionString(connectionString, options.TopicDescription.Path);
            });
        }
        public void StartListening(Func<BrokeredMessage,Task> OnMessageAsync)
        {

            string connectionString = this.options.ConnectionString;

            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(connectionString);

            if (!namespaceManager.TopicExists(this.options.TopicDescription.Path))
            {
                namespaceManager.CreateTopic(this.options.TopicDescription);
            }
            if (!namespaceManager.SubscriptionExists(this.options.TopicDescription.Path, this.options.SubscriptionDescription.Name))
            {
                namespaceManager.CreateSubscription(this.options.SubscriptionDescription);
            }


            SubscriptionClient = SubscriptionClient.CreateFromConnectionString
                (connectionString, this.options.TopicDescription.Path, this.options.SubscriptionDescription.Name);

            this.options.MessageOptions.ExceptionReceived += options_ExceptionReceived;

            SubscriptionClient.OnMessageAsync(OnMessageAsync, this.options.MessageOptions);

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
            var brokeredMessage= new BrokeredMessage(message);
            brokeredMessage.Properties["messageType"] = message.GetType().AssemblyQualifiedName;
            return brokeredMessage;
        }

        private void options_ExceptionReceived(object sender, ExceptionReceivedEventArgs e)
        {
            Trace.TraceError("{0} {1}", e.Exception, e.Exception.InnerException);
        }

        public void Dispose()
        {
            SubscriptionClient.Close();
        }




        public MessageProcessorProviderOptions<BrokeredMessage> Options
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
