using AzureWebRole.MessageProcessor.Core.Notifications;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.Core
{
    public interface IMessageProcessorClient : IDisposable
    {
        Task RestartProcessorAsync();
    }
    public class MessageProcessorClient<MessageType> : IMessageProcessorClient
    {

        private readonly MessageProcessorClientOptions<MessageType> _options;

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
        public Task RestartProcessorAsync()
        {
            CompletedEvent.Set();
            CompletedEvent.Reset();
            
            return StartProcessorAsync();
        }
        private bool resetOnNextIdle = false;
        public void SignalRestartOnNextAllCompletedMessage()
        {
            resetOnNextIdle = true;
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
                _lastMessageRecieved = DateTimeOffset.UtcNow;
                SetIdleCheckTimer();

                if (source != null)
                {
                    source.SetResult(0);
                    source = null;
                }
                CompletedEvent.WaitOne();

                _options.Provider.StopListeningAsync().Wait();
     
            }
            catch (Exception ex)
            {
                source.SetException(ex);

            }
        }


        private DateTimeOffset _lastMessageRecieved = DateTimeOffset.UtcNow;
        private int _isWorking = 0;
        private System.Timers.Timer _idleCheckTimer;

        private void OnIdleCheckTimer(object sender, System.Timers.ElapsedEventArgs e)
        {
            bool restart = false;
            try
            {
                Trace.WriteLine(string.Format("Been running ilde for {0} minutes. Is Working: {1}",
                    DateTimeOffset.UtcNow.Subtract(_lastMessageRecieved).Minutes, _isWorking));
                if (_isWorking == 0)
                {
                    var notice = new IdleRunningNotification(this) { IdleTime = DateTimeOffset.UtcNow.Subtract(_lastMessageRecieved) };
                    _options.Notifications.RunningIdleAsync(notice).Wait();
                                  
                }
            }
            finally
            {
                if(!restart)
                    SetIdleCheckTimer();

            }
        }
        private void SetIdleCheckTimer()
        {
            if (!_options.IdleTimeCheckInterval.HasValue)
                return;


            _idleCheckTimer = new System.Timers.Timer(_options.IdleTimeCheckInterval.Value.TotalMilliseconds);
            _idleCheckTimer.AutoReset = false;
            _idleCheckTimer.Elapsed += new System.Timers.ElapsedEventHandler(OnIdleCheckTimer);
            _idleCheckTimer.Start();
        }


        public static TimeSpan DefaultLockRenewTimer = TimeSpan.FromSeconds(30);
        public async Task OnMessageAsync(MessageType message)
        {
            BaseMessage baseMessage = _options.Provider.FromMessage<BaseMessage>(message);
            baseMessage.MessageId = await _options.Provider.GetMessageIdForMessageAsync(message);
            _lastMessageRecieved = DateTimeOffset.UtcNow;
            Stopwatch sw = Stopwatch.StartNew();

            Trace.WriteLine(string.Format("Starting with message<{0}> : {1}", baseMessage.GetType().Name, baseMessage));

            using (var resolver = _options.ResolverProvider())
            {

                if (await _options.Provider.GetDeliveryCountAsync(message) > _options.Provider.Options.MaxMessageRetries)
                {
                    Trace.TraceInformation("Moving message : {0} to deadletter", message);
                    var moveToDeadLetterEvent = new MovingToDeadLetterNotification(resolver) { Message = baseMessage };

                    if (_options.Notifications != null)
                        await _options.Notifications.MovingMessageToDeadLetterAsync(moveToDeadLetterEvent);

                    if (!moveToDeadLetterEvent.Cancel)
                    {
                        await _options.Provider.MoveToDeadLetterAsync(message, "UnableToProcess", "Failed to process in reasonable attempts");
                        return;
                    }
                }
                Interlocked.Increment(ref _isWorking);

                try
                {
                    bool loop = true;

                    var processingTask = ProcessMessageAsync(baseMessage, resolver);

                    var task = processingTask.ContinueWith((t) => { loop = false; });

                    var timeout = _options.HandlerTimeOut ?? DefaultTimeOut;
                    if (_options.MessageBasedTimeOutProvider != null)
                        timeout = _options.MessageBasedTimeOutProvider(baseMessage) ?? timeout;


                    var MaximumTimeTask = Task.Delay(timeout);
                    while (loop)
                    {
                        var t = await Task.WhenAny(task, Task.Delay(_options.AutoRenewLockTimerDuration ?? DefaultLockRenewTimer), MaximumTimeTask);
                        if (t == MaximumTimeTask)
                            throw new TimeoutException(string.Format("The handler could not finish in given time :{0}", timeout));
                        
                          
                        await _options.Provider.RenewLockAsync(message);
                    }

                    await processingTask; // Make it throw exception

                    Trace.WriteLine(string.Format("Done with message<{0}> : {1}", baseMessage.GetType().Name, baseMessage));
                    //Everything ok, so take it off the queue
                    await _options.Provider.CompleteMessageAsync(message);

                    sw.Stop();
                    if (_options.Notifications != null)
                        await _options.Notifications.MessageCompletedAsync(new MessageCompletedNotification(resolver) { Message = baseMessage, Elapsed = sw.Elapsed });

                }
                finally
                {
                    Interlocked.Decrement(ref _isWorking);
                }
                if(_isWorking == 0 && resetOnNextIdle)
                {
                    CompletedEvent.Set();
                }
                
            }
        }

        public async Task ProcessMessageAsync<T>(T message, IMessageHandlerResolver resolver) where T : BaseMessage
        {

            //Voodoo to construct the right message handler type
            Type handlerType = typeof(IMessageHandler<>);
            Type[] typeArgs = { message.GetType() };
            Type constructed = handlerType.MakeGenericType(typeArgs);
            //NOTE: Could just use reflection here to locate and create an instance
            // of the desired message handler type here if you didn't want to use an IOC container...
            //Get an instance of the message handler type


            // using (var resolver = _options.ResolverProvider())
            // {
            var handler = resolver.GetHandler(constructed);
            if (handler == null)
            {
                var notification = new HandlerNotFoundNotification(resolver) { Message = message, HandlerType = constructed };
                await _options.Notifications.HandlerWasNotFoundAsync(notification);
                if (notification.Handler != null)
                    handler = notification.Handler;

                if (handler == null)
                    throw new Exception(string.Format("The message handler for {0} was not found", constructed));
            }

            var methodInfo = constructed.GetMethods()
                .FirstOrDefault(info => info.Name.Equals("HandleAsync") && info.GetParameters().Any(param => param.ParameterType == typeArgs[0]));

            if (methodInfo == null)
                throw new Exception("HandleAsync not fond for the messagetype");

            var task = methodInfo.Invoke(handler, new[] { message }) as Task;
            if (task != null)
                await task;
            // }
        }



        public void Dispose()
        {
            CompletedEvent.Set(); //Runner shoul now complete.
            _options.Provider.Dispose();

        }

        public static TimeSpan DefaultTimeOut = TimeSpan.FromMinutes(10);
    }
}
