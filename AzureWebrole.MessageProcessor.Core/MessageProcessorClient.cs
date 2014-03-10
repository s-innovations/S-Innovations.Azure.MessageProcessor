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
    public interface IMessageProcessorClientProvider<MessageType> : IMessageProcessorClientProvider<IMessageProcessorProviderOptions<MessageType>, MessageType>
    {
        void StartListening(Func<MessageType,Task> OnMessageAsync);
        Task SendMessageAsync(MessageType message);
        Task SendMessagesAsync(IEnumerable<MessageType> message);

        Task<int> GetDeliveryCountAsync(MessageType message);

        Task CompleteMessageAsync(MessageType message);

        Task MoveToDeadLetterAsync(MessageType message, string p1, string p2);

        Task RenewLockAsync(MessageType message);
    }
    public interface IMessageProcessorClientProvider<T, MessageType> : IDisposable where T : IMessageProcessorProviderOptions<MessageType>
    {
        T Options { get; }
        T FromMessage<T>(MessageType m) where T : BaseMessage;
        MessageType ToMessage<T>(T message) where T : BaseMessage;
    }
    public interface IMessageProcessorProviderOptions<MessageType> 
    {


        int MaxMessageRetries { get; }
        
    }
    public interface IMessageHandlerResolver
    {

        object GetHandler(Type constructed);
    }
    public class MessageProcessorClient<MessageType> : IDisposable
    {
        private readonly IMessageProcessorClientProvider<MessageType> _provider;
        private readonly IMessageHandlerResolver _resolver;
        public MessageProcessorClient(IMessageProcessorClientProvider<MessageType> provider, IMessageHandlerResolver resolver)
        {
            _provider = provider;
            _resolver = resolver;
        }
        private ManualResetEvent CompletedEvent = new ManualResetEvent(false);

        private Task Runner;
        private TaskCompletionSource<int> source;

        public Task StartProcessorAsync()
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

                _provider.StartListening(OnMessageAsync);

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

            if (await _provider.GetDeliveryCountAsync(message) > _provider.Options.MaxMessageRetries)
            {
                await _provider.MoveToDeadLetterAsync(message, "UnableToProcess", "Failed to process in reasonable attempts");
                return;
            }


            BaseMessage baseMessage = _provider.FromMessage<BaseMessage>(message);
            bool loop = true;

            var task = ProcessMessageAsync(baseMessage).ContinueWith((t) => { loop = false; });            
            while (loop)
            {
                await Task.WhenAny(task, Task.Delay(10000));
                await _provider.RenewLockAsync(message);
            }
           

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



        public void Dispose()
        {
            _provider.Dispose();
        }
    }
}
