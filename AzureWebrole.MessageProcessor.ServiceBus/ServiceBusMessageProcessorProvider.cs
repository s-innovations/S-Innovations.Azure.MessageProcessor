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
using System.Threading.Tasks.Dataflow;
using AzureWebRole.MessageProcessor.Core;


namespace AzureWebRole.MessageProcessor.ServiceBus
{
    public interface ServiceBusMessageProcessorClientProvider : IMessageProcessorClientProvider<BrokeredMessage>
    {

    }
    public class ScaledTopicClient
    {
        private const string DEFAULT_COORELATION_ID = "__DEFAULT__";
        //private readonly ServiceBusMessageProcessorProviderOptions options;
        private Random R;
        private int _scaleCount = 1;
        private readonly Dictionary<string, Lazy<TopicClient>[]> LazyTopicClients;
      
        //private readonly NamespaceManager namespaceManager;
        public ScaledTopicClient(ServiceBusMessageProcessorProviderOptions options)
        {
            //   this.options = options;
            this.R = new Random();
            this._scaleCount = options.TopicScaleCount.Value;
            LazyTopicClients = new Dictionary<string, Lazy<TopicClient>[]>();
            if (options.ConnectionStringProvider != null)
            {
                foreach (var mapping in options.ConnectionStringProvider)
                {
                    LazyTopicClients.Add(mapping.Key,
                        CreateTopicClientsForConnectionString(options.TopicScaleCount.Value, options.TopicDescription.Path, mapping.Value));
                }
            }


            LazyTopicClients.Add(DEFAULT_COORELATION_ID, CreateTopicClientsForConnectionString(
                   options.TopicScaleCount.Value, options.TopicDescription.Path, options.ConnectionString));

           
        }

        private Lazy<TopicClient>[] CreateTopicClientsForConnectionString(int count, string prefix, string conn)
        {
            var list = new List<Lazy<TopicClient>>(count);
            var namespaceManager = NamespaceManager.CreateFromConnectionString(conn);

            for (int i = 0, ii = count; i < ii; ++i)
            {
                var name = prefix + i.ToString("D3");
                list.Add(new Lazy<TopicClient>(() => CreateTopicClient(conn, name)));
            }
            return list.ToArray();
        }
        private TopicClient CreateTopicClient(string conn, string topicname)
        {


            Trace.WriteLine(string.Format("Creating Topic Client: {0}, {1}", conn, topicname));

            return TopicClient.CreateFromConnectionString(conn, topicname);
        }

        internal Task SendAsync(BrokeredMessage message)
        {

            int r = R.Next(_scaleCount);
            TopicClient client=  GetClient(message.CorrelationId, r);

            Trace.WriteLine(string.Format("Posting Message onto Topic {1} '{0}'",
                client.Path, r));

            return client.SendAsync(message);

        }

        private TopicClient GetClient(string coorid, int r)
        {


            var coorelationId = coorid ?? DEFAULT_COORELATION_ID;

           return (this.LazyTopicClients.ContainsKey(coorelationId) ?
                this.LazyTopicClients[coorelationId][r] :
                this.LazyTopicClients[DEFAULT_COORELATION_ID][r]).Value;
        }
        internal Task SendBatchAsync(IEnumerable<BrokeredMessage> messages)
        {
            var postBlock = new ActionBlock<IGrouping<string, BrokeredMessage>>((group) =>
            {

                var r = R.Next(_scaleCount);
                TopicClient client = GetClient(group.Key, r);

                Trace.WriteLine(string.Format("Posting Messages onto Topic {1} '{0}'",client.Path,r));

                return client.SendBatchAsync(messages);
            });


            foreach (var group in messages.GroupBy(m => m.CorrelationId ?? DEFAULT_COORELATION_ID))
                postBlock.Post(group);

            postBlock.Complete();
            return postBlock.Completion;

        }
    }
    public class ServiceBusMessageProcessorProvider : ServiceBusMessageProcessorClientProvider
    {

        private readonly ServiceBusMessageProcessorProviderOptions options;

        private MessageClientEntity Client;
        private readonly Lazy<TopicClient> LazyTopicClient;
        private readonly Lazy<QueueClient> LazyQueueClient;
        private readonly ScaledTopicClient ScaledTopicClient;

        public bool SupportTopic { get { return options.TopicDescription != null; } }
        public bool SupportQueue { get { return options.QueueDescription != null; } }
        public bool SupportSubscription { get { return options.SubscriptionDescription != null; } }
        public bool SupportFilteredTopic { get { return options.TopicScaleCount.HasValue; } }


        public ServiceBusMessageProcessorProvider(ServiceBusMessageProcessorProviderOptions options)
        {
            this.options = options;

            if (SupportTopic)
                LazyTopicClient = new Lazy<TopicClient>(CreateTopicClient);
            if (SupportQueue)
                LazyQueueClient = new Lazy<QueueClient>(CreateQueueClient);
            if (SupportFilteredTopic)
            {
                ScaledTopicClient = new ScaledTopicClient(options);
            }


        }

        private QueueClient CreateQueueClient()
        {
           
            var namespaceManager =
               NamespaceManager.CreateFromConnectionString(this.options.ConnectionString);

            if (!namespaceManager.QueueExists(this.options.QueueDescription.Path))
            {
                namespaceManager.CreateQueue(this.options.QueueDescription);
            }
            Trace.WriteLine(string.Format("Creating Queue Client for {0} at {1}", this.options.QueueDescription.Path,namespaceManager.Address));
            var client = QueueClient.CreateFromConnectionString(this.options.ConnectionString, this.options.QueueDescription.Path);
           
            return client;
        }

        private TopicClient CreateTopicClient()
        {
            Trace.WriteLine(string.Format("Creating Topic Client: {0} for Path: {1}", this.options.ConnectionString, this.options.TopicDescription.Path));
            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(this.options.ConnectionString);

            if (!namespaceManager.TopicExists(this.options.TopicDescription.Path))
            {
                namespaceManager.CreateTopic(this.options.TopicDescription);

            }

            return TopicClient.CreateFromConnectionString(this.options.ConnectionString, this.options.TopicDescription.Path);
        }

        private NamespaceManager GetNamespaceManagerForCorrelationId(string id = null)
        {
            if (options.ConnectionStringProvider != null && id != null)
            {
                if (options.ConnectionStringProvider.ContainsKey(id))
                    return NamespaceManager.CreateFromConnectionString(options.ConnectionStringProvider[id]);
            }
            return NamespaceManager.CreateFromConnectionString(this.options.ConnectionString);
        }
        /// <summary>
        /// Creates all the topics,subscriptions for Filtered Queues
        /// </summary>
        /// <returns></returns>
        public async Task EnsureTopicsAndQueuesCreatedAsync()
        {

            var originalSubscriptionName = options.SubscriptionDescription.Name;
            var originalTopicPath = options.TopicDescription.Path;

            //Create All Grouping Targets
            await Task.WhenAll(options.CorrelationToQueueMapping
                .Select(mapping => CreateTargetIfNotExist(GetNamespaceManagerForCorrelationId(mapping.Key), mapping.Value)));

            Func<int, NamespaceManager, Task> EnsureTopicCreated = async (i, namespaceManager) =>
            {
                options.TopicDescription.Path = originalTopicPath + i.ToString("D3");
                if (!await namespaceManager.TopicExistsAsync(options.TopicDescription.Path))
                {
                    await namespaceManager.CreateTopicAsync(options.TopicDescription.Path);
                }
            };

            Trace.TraceInformation("Creating {0} Topics", options.TopicScaleCount.Value);

            for (int i = 0, ii = options.TopicScaleCount.Value; i < ii; ++i)
            {

                Trace.TraceInformation("With '{0}' as filters.", string.Join(", ", options.CorrelationToQueueMapping.Keys));

                foreach (var group in options.CorrelationToQueueMapping
                    .Select(mapping => new
                    {
                        Map = mapping,
                        NameSpaceManager = GetNamespaceManagerForCorrelationId(mapping.Key)
                    }).GroupBy(mapping => mapping.NameSpaceManager.Address.AbsoluteUri))
                {
                    Trace.TraceInformation("With {0} ConnectionString Groups", group.Count());
                    foreach (var mapping in group)
                    {

                        var namespaceManager = GetNamespaceManagerForCorrelationId(mapping.Map.Key);

                        await EnsureTopicCreated(i, namespaceManager);

                        var forwardPath = "";

                        var queue = mapping.Map.Value as QueueDescription;
                        if (queue != null)
                            forwardPath = queue.Path;

                        var topic = mapping.Map.Value as TopicDescription;
                        if (topic != null)
                            forwardPath = topic.Path;

                        Trace.TraceInformation("Found Forward Path {0}", forwardPath);

                        options.SubscriptionDescription.Name = originalSubscriptionName + "2" + forwardPath;
                        options.SubscriptionDescription.TopicPath = options.TopicDescription.Path;
                        options.SubscriptionDescription.ForwardTo = forwardPath;


                        if (!await namespaceManager.SubscriptionExistsAsync(options.TopicDescription.Path, options.SubscriptionDescription.Name))
                        {
                            Trace.TraceInformation("Creating Subscription {0}", options.SubscriptionDescription.Name);
                            await namespaceManager.CreateSubscriptionAsync(options.SubscriptionDescription, new CorrelationFilter(mapping.Map.Key));
                        }
                        else
                        {
                            Trace.TraceInformation("Subscription {0} Already Created", options.SubscriptionDescription.Name);
                        }

                    }

                }

            }
            options.TopicDescription.Path = originalTopicPath;
            options.SubscriptionDescription.Name = originalSubscriptionName;

        }
        private async Task CreateTargetIfNotExist(NamespaceManager manager, EntityDescription entity)
        {
            var queue = entity as QueueDescription;
            if (queue != null)
            {
                if (!await manager.QueueExistsAsync(queue.Path))
                {
                    await manager.CreateQueueAsync(queue);
                }
                return;
            }
            var topic = entity as TopicDescription;
            if (topic != null)
            {
                if (!await manager.TopicExistsAsync(topic.Path))
                {
                    await manager.CreateTopicAsync(topic);
                }
                return;
            }

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
            
            var messageOptions = new OnMessageOptions {  
                MaxConcurrentCalls = this.options.MaxConcurrentProcesses, 
                AutoComplete = false, 
            };
           // if (Options.AutoRenewLockTime.HasValue)
           //     messageOptions.AutoRenewTimeout = Options.AutoRenewLockTime.Value;

            messageOptions.ExceptionReceived += options_ExceptionReceived;


            //Make sure that queues are created first if the subscription could be a forward
            try
            {
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

            if(Client != null)
            {
                Client.RetryPolicy = RetryExponential.Default;
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

            if (options.CorrelationIdProvider != null)
                brokeredMessage.CorrelationId = options.CorrelationIdProvider(message);

            return brokeredMessage;
        }
        //private string makeGuidFromString(string input)
        //{
        //    var provider = new MD5CryptoServiceProvider();
        //    var inputBytes = Encoding.UTF8.GetBytes(input);

        //    var hashBytes = provider.ComputeHash(inputBytes);
        //    var hashGuid = new Guid(hashBytes);

        //    return hashGuid.ToString();
        //}
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
            if (SupportFilteredTopic)
                return ScaledTopicClient.SendAsync(message);
            return LazyTopicClient.Value.SendAsync(message);
        }
        private Task SendMessagesAsync(IEnumerable<BrokeredMessage> messages)
        {
            if (SupportFilteredTopic)
                return ScaledTopicClient.SendBatchAsync(messages);

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


        public Task<string> GetMessageIdForMessageAsync(BrokeredMessage message)
        {
            return Task.FromResult(message.MessageId);
        }


        public async Task StopListeningAsync()
        {
            var client = Client;
            Client = null;

            if (client != null)
            {
                await client.CloseAsync();               
            }


        }
    }
}
