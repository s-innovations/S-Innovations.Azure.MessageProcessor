using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using SInnovations.Azure.MessageProcessor.Core.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SInnovations.Azure.MessageProcessor.ServiceBus
{
    public class ScaledTopicClient
    {
        static readonly ILog Logger = LogProvider.GetCurrentClassLogger();

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


            Logger.TraceFormat("Creating Topic Client: {0}, {1}", conn, topicname);

            return TopicClient.CreateFromConnectionString(conn, topicname);
        }

        internal Task SendAsync(BrokeredMessage message)
        {

            int r = R.Next(_scaleCount);
            TopicClient client = GetClient(message.CorrelationId, r);

            Logger.TraceFormat(string.Format("Posting Message onto Topic {1} '{0}'",
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

                Logger.TraceFormat("Posting Messages onto Topic {1} '{0}'", client.Path, r);

                return client.SendBatchAsync(messages);
            });


            foreach (var group in messages.GroupBy(m => m.CorrelationId ?? DEFAULT_COORELATION_ID))
                postBlock.Post(group);

            postBlock.Complete();
            return postBlock.Completion;

        }
    }
}
