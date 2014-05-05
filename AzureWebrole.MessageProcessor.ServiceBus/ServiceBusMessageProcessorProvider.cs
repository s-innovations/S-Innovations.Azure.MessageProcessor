using AzureWebrole.MessageProcessor.Core;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Linq;
using System.Threading.Tasks;
using System.Security.Cryptography;
using System.Text;


namespace AzureWebRole.MessageProcessor.ServiceBus
{
    public interface ServiceBusMessageProcessorClientProvider : IMessageProcessorClientProvider<BrokeredMessage>
    {

    }
    public class ServiceBusMessageProcessorProvider : ServiceBusMessageProcessorClientProvider
    {

        private readonly ServiceBusMessageProcessorProviderOptions options;

        private MessageClientEntity Client;
        private readonly Lazy<TopicClient> LazyTopicClient;
        private readonly Lazy<QueueClient> LazyQueueClient;

        public bool SupportTopic { get { return options.TopicDescription != null; } }
        public bool SupportQueue { get { return options.QueueDescription != null; } }
        public bool SupportSubscription { get { return options.TopicDescription != null && options.SubscriptionDescription != null; } }

        public ServiceBusMessageProcessorProvider(ServiceBusMessageProcessorProviderOptions options)
        {
            this.options = options;

            if (SupportTopic)
                LazyTopicClient = new Lazy<TopicClient>(CreateTopicClient);
            if (SupportQueue)
                LazyQueueClient = new Lazy<QueueClient>(CreateQueueClient);
        }

        private QueueClient CreateQueueClient()
        {
            Trace.WriteLine("Creating Queue Client");
            var namespaceManager =
               NamespaceManager.CreateFromConnectionString(this.options.ConnectionString);

            if (!namespaceManager.QueueExists(this.options.QueueDescription.Path))
            {
                namespaceManager.CreateQueue(this.options.QueueDescription);
            }

            return QueueClient.CreateFromConnectionString(this.options.ConnectionString, this.options.QueueDescription.Path);
        }

        private TopicClient CreateTopicClient()
        {
            Trace.WriteLine("Creating Topic Client: {0}", this.options.ConnectionString);
            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(this.options.ConnectionString);

            if (!namespaceManager.TopicExists(this.options.TopicDescription.Path))
            {
                namespaceManager.CreateTopic(this.options.TopicDescription);

            }

            return TopicClient.CreateFromConnectionString(this.options.ConnectionString, this.options.TopicDescription.Path);
        }
        public void StartListening(Func<BrokeredMessage, Task> onMessageAsync)
        {

            string connectionString = this.options.ConnectionString;

            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(connectionString);


            try
            {
                if (SupportTopic && !namespaceManager.TopicExists(this.options.TopicDescription.Path))
                {
                    namespaceManager.CreateTopic(this.options.TopicDescription);
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError("Failed To setup Topic: {0} with {1}", this.options.TopicDescription.Path, ex.ToString());
                throw;
            }
            try
            {
                if (SupportSubscription && !namespaceManager.TopicExists(this.options.SubscriptionDescription.TopicPath))
                {
                    namespaceManager.CreateTopic(this.options.SubscriptionDescription.TopicPath);
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError("Failed To setup Topic: {0} with {1}", this.options.SubscriptionDescription.TopicPath, ex.ToString());
                throw;
            }

            var messageOptions = new OnMessageOptions { MaxConcurrentCalls = this.options.MaxConcurrentProcesses, AutoComplete = false };
            messageOptions.ExceptionReceived += options_ExceptionReceived;


            //Make sure that queues are created first if the subscription could be a forward
            try { 
            if (SupportQueue)
            {
                var client = LazyQueueClient.Value;
                
                if (string.IsNullOrEmpty(this.options.QueueDescription.ForwardTo))
                {
                    client.OnMessageAsync(onMessageAsync, messageOptions);

                    Client = client;
                }
            }
            }
            catch (Exception ex)
            {
                Trace.TraceError("Failed To setup Queue: {0} with {1}", this.options.QueueDescription.Path, ex.ToString());
                throw;
            }



            try
            {
                if (SupportSubscription && !namespaceManager.SubscriptionExists(this.options.SubscriptionDescription.TopicPath, this.options.SubscriptionDescription.Name))
                {
                    namespaceManager.CreateSubscription(this.options.SubscriptionDescription);
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError("Failed To setup subscription: {0} {2} with {1}",
                    this.options.SubscriptionDescription.TopicPath, ex.ToString(), this.options.SubscriptionDescription.Name);
                throw;
            }


            try
            {
                //Only use it if it is not a forward subscription.
                if (SupportSubscription && string.IsNullOrEmpty(this.options.SubscriptionDescription.ForwardTo))
                {
                    var client = SubscriptionClient.CreateFromConnectionString
                      (connectionString, this.options.SubscriptionDescription.TopicPath, this.options.SubscriptionDescription.Name);
                    //  OnMessageAsync(onMessageAsync, messageOptions);
                    client.OnMessageAsync(onMessageAsync, messageOptions);

                    Client = client;
                }
            }
            catch (Exception ex)
            {
                Trace.TraceError("Failed To setup subscription client: {0} {2} with {1}", 
                    this.options.SubscriptionDescription.TopicPath, ex.ToString(),
                    this.options.SubscriptionDescription.Name);
                throw;
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
        private BrokeredMessage ToMessage<T>(T message) where T : BaseMessage
        {
            var brokeredMessage = new BrokeredMessage(message);
            var typename = message.GetType().AssemblyQualifiedName;
            brokeredMessage.Properties["messageType"] = typename;
            brokeredMessage.CorrelationId = makeGuidFromString(typename);
            return brokeredMessage;
        }
        private string makeGuidFromString(string input)
        {
            var provider = new MD5CryptoServiceProvider();
            var inputBytes = Encoding.UTF8.GetBytes(input);

            var hashBytes = provider.ComputeHash(inputBytes);
            var hashGuid = new Guid(hashBytes);

            return hashGuid.ToString();
        }
        private void options_ExceptionReceived(object sender, ExceptionReceivedEventArgs e)
        {
            if (e.Exception != null)
                Trace.TraceError("{0} {1}", e.Exception, e.Exception.InnerException);

        }

        public void Dispose()
        {
            if (Client != null)
                Client.Close();
        }




        public IMessageProcessorProviderOptions<BrokeredMessage> Options
        {
            get { return options; }
        }


        private Task SendMessageAsync(BrokeredMessage message)
        {
            return LazyTopicClient.Value.SendAsync(message);
        }
        private Task SendMessagesAsync(IEnumerable<BrokeredMessage> messages)
        {
            return LazyTopicClient.Value.SendBatchAsync(messages);
        }

        public Task SendMessageAsync<T>(T message) where T : BaseMessage
        {
            return SendMessageAsync(ToMessage<T>(message));
        }
        public Task SendMessagesAsync<T>(IEnumerable<T> messages) where T : BaseMessage
        {
            return SendMessagesAsync(messages.Select(ToMessage<T>));
        }



        public Task<int> GetDeliveryCountAsync(BrokeredMessage message)
        {
            return Task.FromResult(message.DeliveryCount);

        }

        public Task CompleteMessageAsync(BrokeredMessage message)
        {
            return message.CompleteAsync();
        }


        public Task MoveToDeadLetterAsync(BrokeredMessage message, string p1, string p2)
        {
            return message.DeadLetterAsync(p1, p2);

        }


        public Task RenewLockAsync(BrokeredMessage message)
        {
            return message.RenewLockAsync();
        }
    }
}
