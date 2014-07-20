using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AzureWebrole.MessageProcessor.Core
{

    public interface IMessageHandler<MessageType> where MessageType : BaseMessage
    {
        Task HandleAsync(MessageType T);
    }
    public interface IMessageProcessorClientProvider<MessageType> : IMessageProcessorClientProvider<IMessageProcessorProviderOptions<MessageType>, MessageType>
    {
        void StartListening(Func<MessageType, Task> OnMessageAsync);
        //Task SendMessageAsync(MessageType message);
        //Task SendMessagesAsync(IEnumerable<MessageType> message);
        Task SendMessageAsync<T>(T message) where T : BaseMessage;
        Task SendMessagesAsync<T>(IEnumerable<T> messages) where T : BaseMessage;

        Task<int> GetDeliveryCountAsync(MessageType message);

        Task CompleteMessageAsync(MessageType message);

        Task MoveToDeadLetterAsync(MessageType message, string p1, string p2);

        Task RenewLockAsync(MessageType message);

        Task<Guid> GetMessageIdForMessageAsync(MessageType message);
    }
    public interface IMessageProcessorClientProvider<TOptions, MessageType> : IDisposable where TOptions : IMessageProcessorProviderOptions<MessageType>
    {
        TOptions Options { get; }
        T FromMessage<T>(MessageType m) where T : BaseMessage;
        // MessageType ToMessage<T>(T message) where T : BaseMessage;
    }
    public interface IMessageProcessorProviderOptions<MessageType>
    {
        int MaxMessageRetries { get; }
        TimeSpan? AutoRenewLockTime { get; }
    }
    public interface IMessageHandlerResolver : IDisposable
    {

        object GetHandler(Type constructed);
    }
    public class MovingToDeadLetter
    {
        public bool Cancel { get; set; }
        public BaseMessage Message { get; set; }
    }
    public class MessageCompletedNotification
    {
        public BaseMessage Message { get; set; }
    }
    public interface IMessageProcessorNotifications
    {
      
     Task MovingMessageToDeadLetterAsync(MovingToDeadLetter moveToDeadLetterEvent);
     Task MessageCompletedAsync(MessageCompletedNotification messageCompletedNotification);
    
    }

    public class DefaultNotifications : IMessageProcessorNotifications
    {
         Func<MovingToDeadLetter,Task> MovingMessageToDeadLetterFunc { get; set; }
         Func<MessageCompletedNotification,Task> MessageCompletedFunc { get; set; }

         public Task MovingMessageToDeadLetterAsync(MovingToDeadLetter moveToDeadLetterEvent)
         {
             if (MovingMessageToDeadLetterFunc != null)
                 return MovingMessageToDeadLetterFunc(moveToDeadLetterEvent);
             return Task.FromResult(0);
         }

         public Task MessageCompletedAsync(MessageCompletedNotification messageCompletedNotification)
         {
             if (MessageCompletedFunc != null)
                 return MessageCompletedFunc(messageCompletedNotification);
             return Task.FromResult(0);
         }
    }
    public class MessageProcessorClientOptions<MessageType>
    {
        public MessageProcessorClientOptions()
        {
            Notifications = new DefaultNotifications();
        }
        public IMessageProcessorNotifications Notifications { get; set; }
        public IMessageProcessorClientProvider<MessageType> Provider{get;set;}

        public Func<IMessageHandlerResolver> ResolverProvider { get; set; }

        public TimeSpan? IdleTimeCheckInterval { get; set; }
    }
    public class MessageProcessorClient<MessageType> : IDisposable
    {
     //   private readonly IMessageProcessorClientProvider<MessageType> _provider;
    //    private readonly Func<IMessageHandlerResolver> _resolverProvider;
       private readonly MessageProcessorClientOptions<MessageType> _options;

      //  public IMessageProcessorNotifications Notifications { get; set; }

        public MessageProcessorClient(MessageProcessorClientOptions<MessageType> options)
        {
           // _provider = provider;
          //  _resolverProvider = resolverProvider;
            _options = options;
         
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
            _options.Provider.SendMessageAsync<T>(Message);
        }
        public void AddMessages<T>(IEnumerable<T> Messages) where T : BaseMessage
        {
            _options.Provider.SendMessagesAsync(Messages);
        }
        private void StartSubscriptionClient()
        {
            try
            {

                _options.Provider.StartListening(OnMessageAsync);
                
                SetIdleCheckTimer();

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


        private DateTimeOffset _lastMessageRecieved;
        private int _isWorking = 0;
        private System.Timers.Timer _idleCheckTimer;
     
        private void OnIdleCheckTimer(object sender, System.Timers.ElapsedEventArgs e)
        {
            try
            {
                Trace.TraceInformation("Been running ilde for {0} minutes. Is Working: {1}", DateTimeOffset.UtcNow.Subtract(_lastMessageRecieved).Minutes, _isWorking);
                
            }
            finally
            {

                SetIdleCheckTimer();

            }
        }
        private void SetIdleCheckTimer()
        {
            if (!_options.IdleTimeCheckInterval.HasValue)
                return;

            Trace.TraceInformation("Setting IdleCheck timer");
            _idleCheckTimer = new System.Timers.Timer(_options.IdleTimeCheckInterval.Value.TotalMilliseconds);
            _idleCheckTimer.AutoReset = false;
            _idleCheckTimer.Elapsed += new System.Timers.ElapsedEventHandler(OnIdleCheckTimer);
            _idleCheckTimer.Start();
        }



        public async Task OnMessageAsync(MessageType message)
        {
            BaseMessage baseMessage = _options.Provider.FromMessage<BaseMessage>(message);
            baseMessage.MessageId = await _options.Provider.GetMessageIdForMessageAsync(message);
            _lastMessageRecieved = DateTimeOffset.UtcNow;
            Interlocked.Increment(ref _isWorking);


            Trace.TraceInformation("Starting with message<{0}> : {1}", baseMessage.GetType().Name, baseMessage);

            if (await _options.Provider.GetDeliveryCountAsync(message) > _options.Provider.Options.MaxMessageRetries)
            {
                Trace.TraceInformation("Moving message : {0} to deadletter", message);
                var moveToDeadLetterEvent = new MovingToDeadLetter() { Message = baseMessage };

                if (_options.Notifications != null)
                    await _options.Notifications.MovingMessageToDeadLetterAsync(moveToDeadLetterEvent);

                if (!moveToDeadLetterEvent.Cancel)
                {
                    await _options.Provider.MoveToDeadLetterAsync(message, "UnableToProcess", "Failed to process in reasonable attempts");
                    return;
                }
            }

                
        //    bool loop = true;

            var processingTask = ProcessMessageAsync(baseMessage);        

            //var task = processingTask.ContinueWith((t) => { loop = false; });

            //while (loop)
            //{
            //    var t = await Task.WhenAny(task, Task.Delay(30000));
            //    if (t != task)
            //        await _options.Provider.RenewLockAsync(message);
            //}

            await processingTask; // Make it throw exception

            Trace.TraceInformation("Done with message<{0}> : {1}", baseMessage.GetType().Name, baseMessage);
            //Everything ok, so take it off the queue
            await _options.Provider.CompleteMessageAsync(message);

            if (_options.Notifications != null)
                await _options.Notifications.MessageCompletedAsync(new MessageCompletedNotification { Message = baseMessage });

            Interlocked.Decrement(ref _isWorking);
        }

        public async Task ProcessMessageAsync<T>(T message) where T : BaseMessage
        {
         
            //Voodoo to construct the right message handler type
            Type handlerType = typeof(IMessageHandler<>);
            Type[] typeArgs = { message.GetType() };
            Type constructed = handlerType.MakeGenericType(typeArgs);
            //NOTE: Could just use reflection here to locate and create an instance
            // of the desired message handler type here if you didn't want to use an IOC container...
            //Get an instance of the message handler type
          
            using (var resolver = _options.ResolverProvider())
            {
                var handler = resolver.GetHandler(constructed);
                Trace.TraceInformation("Got Handler {0}",handler);

                var methodInfo = constructed.GetMethods()
                    .FirstOrDefault(info => info.Name.Equals("HandleAsync") && info.GetParameters().Any(param => param.ParameterType == typeArgs[0]));

                if (methodInfo == null)
                    throw new Exception("HandleAsync not fond for the messagetype");               

                var task = methodInfo.Invoke(handler, new[] { message }) as Task;
                if (task != null)
                    await task;
            }
        }



        public void Dispose()
        {
            CompletedEvent.Set(); //Runner shoul now complete.
            _options.Provider.Dispose();

        }
    }
}
