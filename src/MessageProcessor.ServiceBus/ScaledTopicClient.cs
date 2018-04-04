using Microsoft.Azure.Management.ServiceBus;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Logging;
 
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SInnovations.Azure.MessageProcessor.ServiceBus
{

    public class EntityDescription
    {
        public string Path { get; set; }
    }
    public class TopicDescription : EntityDescription
    {


        public TopicDescription(string path)
        {
            this.Path = path;
        }
    }
    public class SubscriptionDescription : EntityDescription
    {


        public SubscriptionDescription(string topicPath, string name)
        {
            this.TopicPath = topicPath;
            this.Name = name;
        }

        public string Name { get; set; }
        public string TopicPath { get; set; }
        public string ForwardTo { get; set; }
    }
    public class QueueDescription : EntityDescription
    {
        public QueueDescription(string path)
        {
            this.Path = path;
        }

        public string ForwardTo { get; internal set; }
    }
    public class NamespaceManager
    {
        private ServiceBusConnectionStringBuilder sb;
        private ServiceBusManagementClient client;
        private string rg;

        public string namespaceName => sb.Endpoint.Split('.', '/').Skip(2).FirstOrDefault();
        public NamespaceManager(ServiceBusConnectionStringBuilder sb,  ServiceBusManagementClient client, string resourceGroup)
        {
            this.sb = sb;
            this.client = client;
            Address = new Uri(sb.Endpoint);
            this.rg = resourceGroup;
        }

        public Uri Address { get;  set; }

        internal static NamespaceManager CreateFromConnectionString(string conn, ServiceBusManagementClient client, string resourceGroup)
        {
            var sb = new ServiceBusConnectionStringBuilder(conn);
            return new NamespaceManager(sb,client, resourceGroup);
        }

    

        internal async Task<bool> TopicExistsAsync(string path)
        {
            if (client == null)
                return true;

            var topics =await client.Topics.ListByNamespaceAsync(rg, namespaceName);
            return topics.Any(c => c.Name == path);

          
        }

      

        internal async Task CreateTopicAsync(string path)
        {
            await client.Topics.CreateOrUpdateAsync(rg, namespaceName, path, new Microsoft.Azure.Management.ServiceBus.Models.SBTopic
            {
                   EnableExpress = true, 
            });

            
        }

        internal async Task<bool> SubscriptionExistsAsync(string path, string name)
        {
            if (client == null)
                return true;

            var topics = await client.Subscriptions.ListByTopicAsync(rg, namespaceName, path);
            return topics.Any(c => c.Name == name);
        }

        internal async Task CreateSubscriptionAsync(SubscriptionDescription subscriptionDescription, CorrelationFilter correlationFilter)
        {
            await client.Subscriptions.CreateOrUpdateAsync(rg, namespaceName, subscriptionDescription.TopicPath, subscriptionDescription.Name, new Microsoft.Azure.Management.ServiceBus.Models.SBSubscription
            {
               
                  
            });

            var subscriptionClient = new SubscriptionClient(sb.GetNamespaceConnectionString(),subscriptionDescription.TopicPath, subscriptionDescription.Name);

            await subscriptionClient.AddRuleAsync("rule4" + subscriptionDescription.ForwardTo, correlationFilter);

            await client.Subscriptions.CreateOrUpdateAsync(rg, namespaceName, subscriptionDescription.TopicPath, subscriptionDescription.Name, new Microsoft.Azure.Management.ServiceBus.Models.SBSubscription
            {
                ForwardTo = subscriptionDescription.ForwardTo
                
            });

        }
        internal async Task CreateCorrelationFilterAsync(SubscriptionDescription subscriptionDescription, CorrelationFilter correlationFilter)
        {

            await client.Subscriptions.CreateOrUpdateAsync(rg, namespaceName, subscriptionDescription.TopicPath, subscriptionDescription.Name, new Microsoft.Azure.Management.ServiceBus.Models.SBSubscription
            {
                ForwardTo = null

            });

            // await client.Subscriptions.DeleteAsync(rg, namespaceName, subscriptionDescription.TopicPath, subscriptionDescription.Name);

            //await CreateSubscriptionAsync(subscriptionDescription, correlationFilter);

            //await client.Subscriptions.CreateOrUpdateAsync(rg, namespaceName, subscriptionDescription.TopicPath, subscriptionDescription.Name, new Microsoft.Azure.Management.ServiceBus.Models.SBSubscription
            //{


            //});

            var subscriptionClient = new SubscriptionClient(sb.GetNamespaceConnectionString(), subscriptionDescription.TopicPath, subscriptionDescription.Name);

            var rules=await subscriptionClient.GetRulesAsync();

            foreach (var rule in rules) {
                await subscriptionClient.RemoveRuleAsync(rule.Name);
                    
                    }

            await subscriptionClient.AddRuleAsync("rule4" + subscriptionDescription.ForwardTo, correlationFilter);

            await client.Subscriptions.CreateOrUpdateAsync(rg, namespaceName, subscriptionDescription.TopicPath, subscriptionDescription.Name, new Microsoft.Azure.Management.ServiceBus.Models.SBSubscription
            {
                ForwardTo = subscriptionDescription.ForwardTo

            });

        }


        internal async Task<bool> QueueExistsAsync(string path)
        {
            if (client == null)
                return true;

            var topics = await client.Queues.ListByNamespaceAsync(rg, namespaceName);
            return topics.Any(c => c.Name == path);
        }

        internal async Task CreateQueueAsync(QueueDescription queue)
        {
            await client.Queues.CreateOrUpdateAsync(rg, namespaceName, queue.Path, new Microsoft.Azure.Management.ServiceBus.Models.SBQueue
            {
                 ForwardTo = queue.ForwardTo, EnableExpress=true,
            });
        }

        internal async Task CreateTopicAsync(TopicDescription topic)
        {
            await client.Topics.CreateOrUpdateAsync(rg, namespaceName, topic.Path, new Microsoft.Azure.Management.ServiceBus.Models.SBTopic
            {
                 EnableExpress = true,
            });
        }

        internal async Task CreateSubscriptionAsync(SubscriptionDescription subscriptionDescription)
        {
            await client.Subscriptions.CreateOrUpdateAsync(rg, namespaceName, subscriptionDescription.TopicPath, subscriptionDescription.Name, new Microsoft.Azure.Management.ServiceBus.Models.SBSubscription
            {
                ForwardTo = subscriptionDescription.ForwardTo,

            });
        }
    }
    public class ScaledTopicClient
    {
        private readonly ILogger<ScaledTopicClient> _logger;

        private const string DEFAULT_COORELATION_ID = "__DEFAULT__";
        //private readonly ServiceBusMessageProcessorProviderOptions options;
        private Random R;
        private int _scaleCount = 1;
        private readonly Dictionary<string, Lazy<TopicClient>[]> LazyTopicClients;

        //private readonly NamespaceManager namespaceManager;
        public ScaledTopicClient(ILogger<ScaledTopicClient> logger, ServiceBusMessageProcessorProviderOptions options)
        {
            this._logger = logger;
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
         //   var namespaceManager = NamespaceManager.CreateFromConnectionString(conn);

            for (int i = 0, ii = count; i < ii; ++i)
            {
                var name = prefix + i.ToString("D3");
                list.Add(new Lazy<TopicClient>(() => CreateTopicClient(conn, name)));
            }
            return list.ToArray();
        }
        private TopicClient CreateTopicClient(string conn, string topicname)
        {


            _logger.LogTrace("Creating Topic Client: {connection}, {topicname}", conn, topicname);
            return new TopicClient(conn, topicname);
          //  return TopicClient.CreateFromConnectionString(conn, topicname);
        }

        internal Task SendAsync(Message message)
        {

            int r = R.Next(_scaleCount);
            TopicClient client = GetClient(message.CorrelationId, r);

            _logger.LogTrace("Posting Message onto Topic {clientPath} '{clientNumber}'",
                client.Path, r);

            return client.SendAsync(message);

        }

        private TopicClient GetClient(string coorid, int r)
        {


            var coorelationId = coorid ?? DEFAULT_COORELATION_ID;

            return (this.LazyTopicClients.ContainsKey(coorelationId) ?
                 this.LazyTopicClients[coorelationId][r] :
                 this.LazyTopicClients[DEFAULT_COORELATION_ID][r]).Value;
        }
        internal Task SendBatchAsync(IList<Message> messages)
        {
            var postBlock = new ActionBlock<IGrouping<string, Message>>((group) =>
            {

                var r = R.Next(_scaleCount);
                TopicClient client = GetClient(group.Key, r);

                _logger.LogTrace("Posting Messages onto Topic {clientPath} '{clientNumber}'", client.Path, r);

                return client.SendAsync(messages);
            });


            foreach (var group in messages.GroupBy(m => m.CorrelationId ?? DEFAULT_COORELATION_ID))
                postBlock.Post(group);

            postBlock.Complete();
            return postBlock.Completion;

        }
    }
}
