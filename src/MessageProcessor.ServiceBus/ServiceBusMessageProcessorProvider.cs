using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using SInnovations.Azure.MessageProcessor.Core;
using SInnovations.Azure.MessageProcessor.Core.Schedules;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;


namespace SInnovations.Azure.MessageProcessor.ServiceBus
{


    public class ServiceBusMessageProcessorProvider : ServiceBusMessageProcessorClientProvider
    {
        private readonly ILogger<ServiceBusMessageProcessorProvider> _logger;
        private readonly ILoggerFactory _loggerFacory;

        private readonly ServiceBusMessageProcessorProviderOptions options;

        private IMessageReceiver Client;
        private readonly Lazy<TopicClient> LazyTopicClient;
        private readonly Lazy<IMessageReceiver> LazyQueueClient;
        private readonly ScaledTopicClient ScaledTopicClient;

        public bool SupportTopic { get { return options.TopicDescription != null; } }
        public bool SupportQueue { get { return options.QueueDescription != null; } }
        public bool SupportSubscription { get { return options.SubscriptionDescription != null; } }
        public bool SupportFilteredTopic { get { return options.TopicScaleCount.HasValue; } }


        public ServiceBusMessageProcessorProvider(ILoggerFactory loggerFacory, ServiceBusMessageProcessorProviderOptions options)
        {
            this._loggerFacory = loggerFacory;
            this._logger = loggerFacory.CreateLogger<ServiceBusMessageProcessorProvider>();
            this.options = options;

            if (SupportTopic)
                LazyTopicClient = new Lazy<TopicClient>(CreateTopicClient);
            if (SupportQueue)
                LazyQueueClient = new Lazy<IMessageReceiver>(CreateQueueClient);
            if (SupportFilteredTopic)
            {
                ScaledTopicClient = new ScaledTopicClient(loggerFacory.CreateLogger<ScaledTopicClient>(), options);
            }


        }

        private IMessageReceiver CreateQueueClient()
        {

            var namespaceManager =
               NamespaceManager.CreateFromConnectionString(this.options.ConnectionString, this.options.Client, this.options.ResourceGroup);

            if (!namespaceManager.QueueExistsAsync(this.options.QueueDescription.Path).Result)
            {
                namespaceManager.CreateQueueAsync(this.options.QueueDescription).Wait();
            }
            Trace.WriteLine(string.Format("Creating Queue Client for {0} at {1}", this.options.QueueDescription.Path, namespaceManager.Address));
            var client = new MessageReceiver(this.options.ConnectionString, this.options.QueueDescription.Path);

            return client;
        }

        private TopicClient CreateTopicClient()
        {
            Trace.WriteLine(string.Format("Creating Topic Client: {0} for Path: {1}", this.options.ConnectionString, this.options.TopicDescription.Path));
            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(this.options.ConnectionString, this.options.Client, this.options.ResourceGroup);

            if (!namespaceManager.TopicExistsAsync(this.options.TopicDescription.Path).Result)
            {
                namespaceManager.CreateTopicAsync(this.options.TopicDescription).Wait();

            }

            return new TopicClient(this.options.ConnectionString, this.options.TopicDescription.Path);
            // return TopicClient.CreateFromConnectionString(this.options.ConnectionString, this.options.TopicDescription.Path);
        }

        private NamespaceManager GetNamespaceManagerForCorrelationId(string id = null)
        {
            if (options.ConnectionStringProvider != null && id != null)
            {
                if (options.ConnectionStringProvider.ContainsKey(id))
                    return NamespaceManager.CreateFromConnectionString(options.ConnectionStringProvider[id], options.Client, this.options.ResourceGroup);
            }
            return NamespaceManager.CreateFromConnectionString(this.options.ConnectionString, options.Client, this.options.ResourceGroup);
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

            _logger.LogInformation("Creating {topicScaleCount} Topics", options.TopicScaleCount.Value);

            for (int i = 0, ii = options.TopicScaleCount.Value; i < ii; ++i)
            {

                _logger.LogInformation("With '{correlationKeys}' as filters.", string.Join(", ", options.CorrelationToQueueMapping.Keys));

                foreach (var group in options.CorrelationToQueueMapping
                    .Select(mapping => new
                    {
                        Map = mapping,
                        NameSpaceManager = GetNamespaceManagerForCorrelationId(mapping.Key)
                    }).GroupBy(mapping => mapping.NameSpaceManager.Address.AbsoluteUri))
                {
                    _logger.LogInformation("With {groupCount} ConnectionString Groups", group.Count());
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

                        _logger.LogInformation("Found Forward Path {forwardPath}", forwardPath);

                        options.SubscriptionDescription.Name = originalSubscriptionName + "2" + forwardPath;
                        options.SubscriptionDescription.TopicPath = options.TopicDescription.Path;
                        options.SubscriptionDescription.ForwardTo = forwardPath;


                        if (!await namespaceManager.SubscriptionExistsAsync(options.TopicDescription.Path, options.SubscriptionDescription.Name))
                        {
                            _logger.LogInformation("Creating Subscription {subscriptionName}", options.SubscriptionDescription.Name);
                            await namespaceManager.CreateSubscriptionAsync(options.SubscriptionDescription, new CorrelationFilter(mapping.Map.Key));
                        }
                        else
                        {
                            _logger.LogInformation("Subscription {subscriptionName} Already Created", options.SubscriptionDescription.Name);
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
       // PropertyInfo InnerReceiverQueueClient = typeof(QueueClient).GetProperty("InnerReceiver", BindingFlags.Instance | BindingFlags.NonPublic);
      //  PropertyInfo InnerSubscriptionClient = typeof(SubscriptionClient).GetProperty("InnerSubscriptionClient", BindingFlags.Instance | BindingFlags.NonPublic);

        public async Task StartListeningAsync(Func<Message, CancellationToken, Task> onMessageAsync)
        {



            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(this.options.ConnectionString, this.options.Client, this.options.ResourceGroup);


            try
            {
                if (SupportTopic && !await namespaceManager.TopicExistsAsync(this.options.TopicDescription.Path))
                {
                    await namespaceManager.CreateTopicAsync(this.options.TopicDescription);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed To setup Topic: {topicPath}", this.options.TopicDescription.Path);
                throw;
            }
            try
            {
                if (SupportSubscription && !await namespaceManager.TopicExistsAsync(this.options.SubscriptionDescription.TopicPath))
                {
                    await namespaceManager.CreateTopicAsync(this.options.SubscriptionDescription.TopicPath);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed To setup Topic: {topicPath}", this.options.SubscriptionDescription.TopicPath);
                throw;
            }

            var messageOptions = new MessageHandlerOptions(options_ExceptionReceived)
            {
                MaxConcurrentCalls = this.options.MaxConcurrentProcesses,
                AutoComplete = false,
            };
            // if (Options.AutoRenewLockTime.HasValue)
            //     messageOptions.AutoRenewTimeout = Options.AutoRenewLockTime.Value;

            //  messageOptions.ExceptionReceivedHandler += options_ExceptionReceived;


            //Make sure that queues are created first if the subscription could be a forward
            try
            {
                if (SupportQueue)
                {
                    var client = LazyQueueClient.Value;

                    if (string.IsNullOrEmpty(this.options.QueueDescription.ForwardTo))
                    {
                        //  client.OnMessageAsync(onMessageAsync, messageOptions);
                        //                        client.RegisterMessageHandler(onMessageAsync, messageOptions);

                        // Client = (MessageReceiver)InnerReceiverQueueClient.GetValue(client);
                        Client = client;// new MessageReceiver(this.options.ConnectionString, $"{this.options.SubscriptionDescription.TopicPath}/{this.options.SubscriptionDescription.Name}");
                        Client.RegisterMessageHandler(onMessageAsync, messageOptions);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed To setup Queue: {queueName}",
                    this.options.QueueDescription.Path);
                throw;
            }



            try
            {
                if (SupportSubscription && !await namespaceManager.SubscriptionExistsAsync(this.options.SubscriptionDescription.TopicPath, this.options.SubscriptionDescription.Name))
                {
                    await namespaceManager.CreateSubscriptionAsync(this.options.SubscriptionDescription);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed To setup subscription: {topicPath} {subscriptionName}",
                    this.options.SubscriptionDescription.TopicPath, this.options.SubscriptionDescription.Name);
                throw;
            }


            try
            {
                //Only use it if it is not a forward subscription.
                if (SupportSubscription && string.IsNullOrEmpty(this.options.SubscriptionDescription.ForwardTo))
                {
                    //var client = new SubscriptionClient// SubscriptionClient.CreateFromConnectionString
                    //  (this.options.ConnectionString, this.options.SubscriptionDescription.TopicPath, this.options.SubscriptionDescription.Name);
                    ////  OnMessageAsync(onMessageAsync, messageOptions);
                    //client.RegisterMessageHandler(onMessageAsync, messageOptions);
                    ////  client.OnMessageAsync(onMessageAsync, messageOptions);
                    //var a = InnerSubscriptionClient.GetValue(client);
                    //;
                    //Client = (MessageReceiver)a.GetType().GetProperty("InnerReceiver", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(a);

                    Client = new MessageReceiver(this.options.ConnectionString, $"{this.options.SubscriptionDescription.TopicPath}/{this.options.SubscriptionDescription.Name}");
                    Client.RegisterMessageHandler(onMessageAsync, messageOptions);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed To setup subscription client: {topicPath} {subscriptionName}",
                    this.options.SubscriptionDescription.TopicPath, ex.ToString(),
                    this.options.SubscriptionDescription.Name);
                throw;
            }

            //if (Client != null)
            //{
            //    Client.RetryPolicy = RetryExponential.Default;
            //}

        }
        //   static MethodInfo method = typeof(BrokeredMessage).GetMethod("GetBody", new Type[] { });
        public async Task<BaseMessage> FromMessageAsync(Message message)
        {

            if (message.UserProperties.ContainsKey("messageType"))
            {
                var messageBodyType =
                          Type.GetType(message.UserProperties["messageType"].ToString());
                if (messageBodyType == null)
                {
                    //Should never get here as a messagebodytype should
                    //always be set BEFORE putting the message on the queue
                    _logger.LogError("MessageType could not be loaded.message {messageId}, {messageType}", message.MessageId, message.UserProperties["messageType"].ToString());
                    // m.DeadLetter();
                    throw new Exception(string.Format("MessageType could not be loaded.message {0}, {1}", message.MessageId, message.UserProperties["messageType"].ToString()));
                }

                // MethodInfo generic = method.MakeGenericMethod(messageBodyType);
                //  var messageBody = (BaseMessage)generic.Invoke(message, null);

                using (var gzip = new GZipStream(new MemoryStream(message.Body), CompressionMode.Decompress, true))
                {
                    using (var streamReader = new StreamReader(gzip))
                    {
                        using (var jsonReader = new JsonTextReader(streamReader))
                        {
                            var messageBody = (BaseMessage)serializer.Deserialize(jsonReader, messageBodyType);




                            if (Options.RepositoryProvider != null && messageBody is IModelBasedMessage)
                            {
                                var modelHolder = messageBody as IModelBasedMessage;

                                var repository = Options.RepositoryProvider.GetRepository();
                                await repository.GetModelAsync(modelHolder);

                            }


                            return messageBody;
                        }
                    }
                }
            }

            return new DefaultServiceBusBaseMessage(message);
        }
        private ConcurrentDictionary<Type, Action<Message, object>[]> promoters = new ConcurrentDictionary<Type, Action<Message, object>[]>();

        private Action<Message, object>[] Factory(Type type)
        {

            var actions = new List<Action<Message, object>>();
            PropertyInfo[] properties = type.GetProperties();
            foreach (PropertyInfo prop in properties)
            {
                foreach (PromotedPropertyAttribute promotedProp in prop.GetCustomAttributes(typeof(PromotedPropertyAttribute), true))
                {
                    var name = promotedProp.Name;
                    actions.Add((msg, serializableObject) =>
                    {
                        object value = prop.GetValue(serializableObject, null);
                        _logger.LogTrace("Promoting {name} with {value}", name, value);
                        switch (name)
                        {
                            case "SessionId":
                                msg.SessionId = (string)value;
                                break;
                            case "ScheduledEnqueueTimeUtc":
                                msg.ScheduledEnqueueTimeUtc = (DateTime)value;
                                break;
                            default:
                                msg.UserProperties.Add(name, value);
                                break;
                        }


                    });
                }
            }
            return actions.ToArray();


        }
        static JsonSerializerSettings settings = new JsonSerializerSettings() { TypeNameHandling = TypeNameHandling.All };
        JsonSerializer serializer = JsonSerializer.Create(settings);
        private Message ToMessage<T>(T message) where T : BaseMessage
        {
            var ms = new MemoryStream();
            using (var gzip = new GZipStream(ms, CompressionMode.Compress, true))
            {
                using (var streamWriter = new StreamWriter(gzip))
                {
                    using (var jsonWriter = new JsonTextWriter(streamWriter))
                    {
                        serializer.Serialize(jsonWriter, message);
                    }
                }
            }


            var brokeredMessage = new Message(ms.ToArray());
            var typename = message.GetType().AssemblyQualifiedName;
            brokeredMessage.UserProperties["messageType"] = typename;

            try
            {
                foreach (var promotor in promoters.GetOrAdd(message.GetType(), Factory))
                {
                    promotor(brokeredMessage, message);
                }

            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to promote properties");
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
                if (!string.IsNullOrEmpty(att.TimeBetweenSchedules))
                {
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

        private Task options_ExceptionReceived(ExceptionReceivedEventArgs e)
        {

            if (e.Exception != null)
                _logger.LogError(e.Exception, "Exception recieved {action}", e.ExceptionReceivedContext.Action);
            else
                _logger.LogWarning("Exception recieved {action}", e.ExceptionReceivedContext.Action);
            return Task.CompletedTask;
        }

        public void Dispose()
        {
            if (Client != null)
                Client.CloseAsync().Wait();
        }

        public IMessageProcessorProviderOptions<Message> Options
        {
            get { return options; }
        }


        private Task SendMessageAsync(Message message)
        {
            if (SupportFilteredTopic)
                return ScaledTopicClient.SendAsync(message);
            return LazyTopicClient.Value.SendAsync(message);
        }
        private Task SendMessagesAsync(IList<Message> messages)
        {

            if (SupportFilteredTopic)
                return ScaledTopicClient.SendBatchAsync(messages);

            return LazyTopicClient.Value.SendAsync(messages);
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
            await SendMessagesAsync(messages.Select(ToMessage<T>).ToList());
        }



        public Task<int> GetDeliveryCountAsync(Message message)
        {
            return Task.FromResult(message.SystemProperties.DeliveryCount);

        }

        public Task CompleteMessageAsync(Message message)
        {
            //    return messageReciever.CompleteAsync(message.SystemProperties.LockToken);
            return Client.CompleteAsync(message.SystemProperties.LockToken);
            //   return message.CompleteAsync();
        }


        public Task MoveToDeadLetterAsync(Message message, string p1, string p2)
        {
            return Client.DeadLetterAsync(message.SystemProperties.LockToken, p1, p2);
            //  return message.DeadLetterAsync(p1, p2);

        }


        public Task RenewLockAsync(Message message)
        {
            return Client.RenewLockAsync(message);

            //   return message.RenewLockAsync();
        }


        public Task<string> GetMessageIdForMessageAsync(Message message)
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



        public Task<DateTime> GetEnqueuedTimeUtcAsync(Message message)
        {
            return Task.FromResult(message.SystemProperties.EnqueuedTimeUtc);
        }



    }
    public class DefaultServiceBusBaseMessage : BaseMessage
    {
        public Message Message { get; set; }
        public DefaultServiceBusBaseMessage(Message message)
        {
            Message = message;
        }
    }
}
