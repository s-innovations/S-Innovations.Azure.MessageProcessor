using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AzureWebrole.MessageProcessor.Core
{

    public interface IMessageHandler<MessageType>  where MessageType : BaseMessage
    {
        Task HandleAsync(MessageType T);

    }
    public interface MessageProcessorClientProvider<MessageType> : MessageProcessorClientProvider<MessageProcessorProviderOptions<MessageType>, MessageType>
    {
        void StartListening();
        Task SendMessageAsync(MessageType message);
        Task SendMessagesAsync(IEnumerable<MessageType> message);

        int GetDeliveryCount(MessageType message);

        Task CompleteMessageAsync(MessageType message);
    }
    public interface MessageProcessorClientProvider<T, MessageType> : IDisposable where T : MessageProcessorProviderOptions<MessageType>
    {
        T Options { get; }
        T FromMessage<T>(MessageType m) where T : BaseMessage;
        MessageType ToMessage<T>(T message) where T : BaseMessage;
    }
    public interface MessageProcessorProviderOptions<MessageType> 
    {


        int MaxMessageRetries { get; }
        
    }
    public interface MessageHandlerResolver
    {

        object GetHandler(Type constructed);
    }
    public class MessageProcessorClient<MessageType>
    {
        private readonly MessageProcessorClientProvider<MessageType> _provider;
        private readonly MessageHandlerResolver _resolver;
        public MessageProcessorClient(MessageProcessorClientProvider<MessageType> provider, MessageHandlerResolver resolver)
        {
            _provider = provider;
            _resolver = resolver;
        }
        private ManualResetEvent CompletedEvent = new ManualResetEvent(false);

        private Task Runner;
        private TaskCompletionSource<int> source;

        public Task InitializePluginAsync()
        {
            Trace.WriteLine("Starting MessageProcessorClient");
            source = new TaskCompletionSource<int>();
            Runner = Task.Factory.StartNew(StartSubscriptionClient, TaskCreationOptions.LongRunning);
            return source.Task;

        }
        public void AddMessage<T>(T Message) where T : BaseMessage
        {
            _provider.SendMessageAsync(_provider.ToMessage(Message));
        }
        public void AddMessages<T>(IEnumerable<T> Message) where T : BaseMessage
        {
            _provider.SendMessagesAsync( Message.Select(_provider.ToMessage));
        }
        private void StartSubscriptionClient()
        {
            try
            {

                _provider.StartListening();

                if (source != null)
                {
                    source.SetResult(0);
                    source = null;
                }
                CompletedEvent.WaitOne();
            }
            catch (Exception ex)
            {
                source.SetException(ex);

            }
        }

        public async Task OnMessageAsync(MessageType message)
        {

            if (_provider.GetDeliveryCount(message) > _provider.Options.MaxMessageRetries)
                return;


            BaseMessage baseMessage = _provider.FromMessage<BaseMessage>(message);
            await ProcessMessageAsync(baseMessage);
            //Everything ok, so take it off the queue
            await _provider.CompleteMessageAsync(message);

        }

        public Task ProcessMessageAsync<T>(T message) where T : BaseMessage
        {
            //Voodoo to construct the right message handler type
            Type handlerType = typeof(IMessageHandler<>);
            Type[] typeArgs = { message.GetType() };
            Type constructed = handlerType.MakeGenericType(typeArgs);
            //NOTE: Could just use reflection here to locate and create an instance
            // of the desired message handler type here if you didn't want to use an IOC container...
            //Get an instance of the message handler type
            var handler = _resolver.GetHandler(constructed);
            //Handle the message
            var methodInfo = constructed.GetMethod("HandleAsync");
            return methodInfo.Invoke(handler, new[] { message }) as Task;
        }


    }
}
