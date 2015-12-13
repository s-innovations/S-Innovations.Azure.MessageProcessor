using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using SInnovations.Azure.MessageProcessor.Core;
using SInnovations.Azure.MessageProcessor.Core.Logging;
using SInnovations.Azure.MessageProcessor.Core.Schedules;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


namespace SInnovations.Azure.MessageProcessor.ServiceBus
{


    public class ServiceBusMessageProcessorProvider : ServiceBusMessageProcessorClientProvider
    {
        static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

        private readonly ServiceBusMessageProcessorProviderOptions options;

        private ClientEntity Client;
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
            Trace.WriteLine(string.Format("Creating Queue Client for {0} at {1}", this.options.QueueDescription.Path, namespaceManager.Address));
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

            Logger.InfoFormat("Creating {0} Topics", options.TopicScaleCount.Value);

            for (int i = 0, ii = options.TopicScaleCount.Value; i < ii; ++i)
            {

                Logger.InfoFormat("With '{0}' as filters.", string.Join(", ", options.CorrelationToQueueMapping.Keys));

                foreach (var group in options.CorrelationToQueueMapping
                    .Select(mapping => new
                    {
                        Map = mapping,
                        NameSpaceManager = GetNamespaceManagerForCorrelationId(mapping.Key)
                    }).GroupBy(mapping => mapping.NameSpaceManager.Address.AbsoluteUri))
                {
                    Logger.InfoFormat("With {0} ConnectionString Groups", group.Count());
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

                        Logger.InfoFormat("Found Forward Path {0}", forwardPath);

                        options.SubscriptionDescription.Name = originalSubscriptionName + "2" + forwardPath;
                        options.SubscriptionDescription.TopicPath = options.TopicDescription.Path;
                        options.SubscriptionDescription.ForwardTo = forwardPath;


                        if (!await namespaceManager.SubscriptionExistsAsync(options.TopicDescription.Path, options.SubscriptionDescription.Name))
                        {
                            Logger.InfoFormat("Creating Subscription {0}", options.SubscriptionDescription.Name);
                            await namespaceManager.CreateSubscriptionAsync(options.SubscriptionDescription, new CorrelationFilter(mapping.Map.Key));
                        }
                        else
                        {
                            Logger.InfoFormat("Subscription {0} Already Created", options.SubscriptionDescription.Name);
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
                if (options.SessionQueueDescriptionProvider != null)
                {
                    var sessionQueue = options.SessionQueueDescriptionProvider(queue);
                    if (!await manager.QueueExistsAsync(sessionQueue.Path))
                    {
                        await manager.CreateQueueAsync(sessionQueue);
                    }
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
                Logger.ErrorFormat("Failed To setup Topic: {0} with {1}", this.options.TopicDescription.Path, ex.ToString());
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
                Logger.ErrorFormat("Failed To setup Topic: {0} with {1}", this.options.SubscriptionDescription.TopicPath, ex.ToString());
                throw;
            }

            var messageOptions = new OnMessageOptions
            {
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
                Logger.ErrorFormat("Failed To setup Queue: {0} with {1}", this.options.QueueDescription.Path, ex.ToString());
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
                Logger.ErrorFormat("Failed To setup subscription: {0} {2} with {1}",
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
                Logger.ErrorFormat("Failed To setup subscription client: {0} {2} with {1}",
                    this.options.SubscriptionDescription.TopicPath, ex.ToString(),
                    this.options.SubscriptionDescription.Name);
                throw;
            }

            if (Client != null)
            {
                Client.RetryPolicy = RetryExponential.Default;
            }

        }
        static MethodInfo method = typeof(BrokeredMessage).GetMethod("GetBody", new Type[] { });
        public async Task<T> FromMessageAsync<T>(BrokeredMessage m) where T : BaseMessage
        {

            var messageBodyType =
                      Type.GetType(m.Properties["messageType"].ToString());
            if (messageBodyType == null)
            {
                //Should never get here as a messagebodytype should
                //always be set BEFORE putting the message on the queue
                Logger.ErrorFormat("MessageType could not be loaded.message {0}, {1}", m.MessageId, m.Properties["messageType"].ToString());
                // m.DeadLetter();
                throw new Exception(string.Format("MessageType could not be loaded.message {0}, {1}", m.MessageId, m.Properties["messageType"].ToString()));
            }

            MethodInfo generic = method.MakeGenericMethod(messageBodyType);
            var messageBody = (T)generic.Invoke(m, null);


            if (Options.RepositoryProvider != null && messageBody is IModelBasedMessage)
            {
                var modelHolder = messageBody as IModelBasedMessage;

                var repository = Options.RepositoryProvider.GetRepository();
                await repository.GetModelAsync(modelHolder);

            }


            return messageBody;
        }
        private ConcurrentDictionary<Type, Action<BrokeredMessage, object>[]> promoters = new ConcurrentDictionary<Type, Action<BrokeredMessage, object>[]>();

        private Action<BrokeredMessage, object>[] Factory(Type type)
        {

            var actions = new List<Action<BrokeredMessage, object>>();
            PropertyInfo[] properties = type.GetProperties();
            foreach (PropertyInfo prop in properties)
            {
                foreach (PromotedPropertyAttribute promotedProp in prop.GetCustomAttributes(typeof(PromotedPropertyAttribute), true))
                {
                    var name = promotedProp.Name;
                    actions.Add((msg, serializableObject) =>
                    {
                        object value = prop.GetValue(serializableObject, null);
                        Logger.TraceFormat("Promoting {0} with {1}", name, value);
                        switch (name)
                        {
                            case "SessionId":
                                msg.SessionId = (string)value;
                                break;
                            case "ScheduledEnqueueTimeUtc":
                                msg.ScheduledEnqueueTimeUtc = (DateTime)value;
                                break;
                            default:
                                msg.Properties.Add(name, value);
                                break;
                        }


                    });
                }
            }
            return actions.ToArray();


        }
        private BrokeredMessage ToMessage<T>(T message) where T : BaseMessage
        {
            var brokeredMessage = new BrokeredMessage(message);
            var typename = message.GetType().AssemblyQualifiedName;
            brokeredMessage.Properties["messageType"] = typename;
           
            try
            {
                foreach (var promotor in promoters.GetOrAdd(message.GetType(), Factory))
                {
                    promotor(brokeredMessage, message);
                }

            }
            catch (Exception ex)
            {
                Logger.ErrorException("Failed to promote properties", ex);
            }

            if (options.CorrelationIdProvider != null)
                brokeredMessage.CorrelationId = options.CorrelationIdProvider(message);

            DateTime? timeToSend = null;
            if (options.ScheduledEnqueueTimeUtcProvider != null)
            {

                timeToSend = options.ScheduledEnqueueTimeUtcProvider(message);

            }


            if (Attribute.IsDefined(message.GetType(), typeof(MessageScheduleAttribute)))
            {
                var att = message.GetType().GetCustomAttribute<MessageScheduleAttribute>();
                if(!string.IsNullOrEmpty(att.TimeBetweenSchedules)){
                    timeToSend = att.GetNextWindowTime();
                }
            }

            if (timeToSend.HasValue)
            {
                var hashPart = options.ScheduledEnqueueTimeHashProvider != null ? 
                    options.ScheduledEnqueueTimeHashProvider(message) ?? "" : "";

                brokeredMessage.ScheduledEnqueueTimeUtc = timeToSend.Value;
                brokeredMessage.MessageId = HashString(hashPart + typename + brokeredMessage.ScheduledEnqueueTimeUtc.ToString("yyyy-M-ddThh:mm:ss.ff"));
            }

            return brokeredMessage;
        }
        private string HashString(string src)
        {
            byte[] stringbytes = Encoding.UTF8.GetBytes(src);
            byte[] hashedBytes = new System.Security.Cryptography
                .SHA1CryptoServiceProvider()
                .ComputeHash(stringbytes);
            Array.Resize(ref hashedBytes, 16);
            return new Guid(hashedBytes).ToString();
        }

        private void options_ExceptionReceived(object sender, ExceptionReceivedEventArgs e)
        {
            Logger.Info("ExceptionReceived");
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

        public async Task SendMessageAsync<T>(T message) where T : BaseMessage
        {
            if (Options.RepositoryProvider != null && message is IModelBasedMessage)
            {
                var modelHolder = message as IModelBasedMessage;

                var repository = Options.RepositoryProvider.GetRepository();
                await repository.SaveModelAsync(modelHolder);

            }

            await SendMessageAsync(ToMessage<T>(message));
        }
        public async Task SendMessagesAsync<T>(IEnumerable<T> messages) where T : BaseMessage
        {
            if (Options.RepositoryProvider != null)
            {
                var modelBasedMsgs = messages.OfType<IModelBasedMessage>().ToArray();

                var block = new ActionBlock<IModelBasedMessage>(async (modelHolder) =>
                {
                    var repository = Options.RepositoryProvider.GetRepository();
                    await repository.SaveModelAsync(modelHolder);
                });


                foreach (var msg in modelBasedMsgs)
                    block.Post(msg);

                block.Complete();
                await block.Completion;


            }
            await SendMessagesAsync(messages.Select(ToMessage<T>));
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



        public Task<DateTime> GetEnqueuedTimeUtcAsync(BrokeredMessage message)
        {
            return Task.FromResult(message.EnqueuedTimeUtc);
        }



    }
}
