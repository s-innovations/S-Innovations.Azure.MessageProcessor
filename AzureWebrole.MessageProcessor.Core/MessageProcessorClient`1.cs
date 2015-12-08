using SInnovations.Azure.MessageProcessor.Core.Logging;
using SInnovations.Azure.MessageProcessor.Core.Notifications;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Reflection;
using SInnovations.Azure.MessageProcessor.Core.Schedules;

namespace SInnovations.Azure.MessageProcessor.Core
{

    public interface IMessageProcessorClient : IDisposable
    {
        Task RestartProcessorAsync();
        Task StartProcessorAsync();
        void SignalRestartOnNextAllCompletedMessage();

    }

    public class MessageProcessorClient<MessageType> : IMessageProcessorClient
    {
        private static ILog Logger = LogProvider.GetCurrentClassLogger();
        private ManualResetEvent _completeBlocker = new ManualResetEvent(false);
        private bool _resetOnNextIdle = false;

        private readonly MessageProcessorClientOptions<MessageType> _options;
        private Task _runnerTask;
        private TaskCompletionSource<int> _startingCompletionSource;


        public MessageProcessorClient(MessageProcessorClientOptions<MessageType> options)
        {
            _options = options;
        }




        #region Listening Mode
        public Task StartProcessorAsync()
        {
            Logger.Info("Starting Message Processor Client");

            _startingCompletionSource = new TaskCompletionSource<int>();
            _runnerTask = Task.Factory.StartNew(StartSubscriptionClient, TaskCreationOptions.LongRunning);
            return _startingCompletionSource.Task;

        }

        public Task RestartProcessorAsync()
        {
            Logger.Info("Restarting Message Processor Client");

            _completeBlocker.Set();
            _completeBlocker.Reset();

            return StartProcessorAsync();
        }


        public void SignalRestartOnNextAllCompletedMessage()
        {
            _resetOnNextIdle = true;
        }
        #endregion Listening Mode

        #region Sending Mode
        public void AddMessage<T>(T Message) where T : BaseMessage
        {
            _options.Provider.SendMessageAsync<T>(Message);
        }
        public void AddMessages<T>(IEnumerable<T> Messages) where T : BaseMessage
        {
            _options.Provider.SendMessagesAsync(Messages);
        }
        #endregion Sending Mode

        private void StartSubscriptionClient()
        {
            try
            {

                _options.Provider.StartListening(OnMessageAsync);
                _lastMessageRecieved = DateTimeOffset.UtcNow;

                SetIdleCheckTimer();

                if (_startingCompletionSource != null)
                {
                    _startingCompletionSource.SetResult(0);
                    _startingCompletionSource = null;
                }
                _completeBlocker.WaitOne();

                _options.Provider.StopListeningAsync().Wait();

            }
            catch (Exception ex)
            {
                _startingCompletionSource.SetException(ex);

            }
        }


        private DateTimeOffset _lastMessageRecieved = DateTimeOffset.UtcNow;
        private int _isWorking = 0;
        private System.Timers.Timer _idleCheckTimer;
        private Lazy<int> _currentProcessId = new Lazy<int>(() => Process.GetCurrentProcess().Id);
        private void OnIdleCheckTimer(object sender, System.Timers.ElapsedEventArgs e)
        {
            bool restart = false;
            try
            {
                Trace.WriteLine(string.Format("Process {2}: Been running ilde for {0} minutes. Is Working: {1}",
                    DateTimeOffset.UtcNow.Subtract(_lastMessageRecieved).Minutes, _isWorking, _currentProcessId.Value));
                if (_isWorking == 0)
                {
                    var notice = new IdleRunningNotification(this) { IdleTime = DateTimeOffset.UtcNow.Subtract(_lastMessageRecieved) };
                    _options.Notifications.RunningIdleAsync(notice).Wait();

                }
            }
            finally
            {
                if (!restart)
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

            BaseMessage baseMessage = await _options.Provider.FromMessageAsync<BaseMessage>(message);
            baseMessage.MessageId = await _options.Provider.GetMessageIdForMessageAsync(message);
            _lastMessageRecieved = DateTimeOffset.UtcNow;
            var enqued = await _options.Provider.GetEnqueuedTimeUtcAsync(message);
            var transmitTime = DateTime.UtcNow.Subtract(enqued);


            Stopwatch sw = Stopwatch.StartNew();



            using (var resolver = _options.ResolverProvider())
            {
                Interlocked.Increment(ref _isWorking);

                try
                {

                    try
                    {
                        if (_options.Notifications != null)
                            await _options.Notifications.MessageStartedAsync(new MessageStartedNotification(resolver)
                            {
                                Elapsed = transmitTime,
                                Message = baseMessage,
                                WorkingCount = _isWorking,
                            });

                    }catch(Exception ex)
                    {
                        Logger.WarnException("Notification MesssageStarted Failed for messageid {0} : ", ex,baseMessage.MessageId);
                    }

                  
                    Logger.DebugFormat("Starting with message<{0}> : {1}", baseMessage.GetType().Name, baseMessage);

                    if (!await MoveToDeadLetterHandlingAsync(message, baseMessage, resolver))
                    {
                        await ProccessMessageHandlingAsync(message, baseMessage, resolver);

                        Logger.DebugFormat("Done with message<{0}> : {1}", baseMessage.GetType().Name, baseMessage);

                        await FinalizeMessageAsync(message, baseMessage, transmitTime, sw, resolver);
                    }

                    await ReEnQueueMessageAsyncIfNeeded(baseMessage, resolver);


                }
                finally
                {
                    Interlocked.Decrement(ref _isWorking);
                }

                if (_isWorking == 0 && _resetOnNextIdle)
                {
                    _completeBlocker.Set();
                }

            }
        }

        private async Task<bool> MoveToDeadLetterHandlingAsync(MessageType message, BaseMessage baseMessage, IMessageHandlerResolver resolver)
        {
            if (await _options.Provider.GetDeliveryCountAsync(message) > _options.Provider.Options.MaxMessageRetries)
            {
                try
                {
                    var moveToDeadLetterEvent = new MovingToDeadLetterNotification(resolver) { Message = baseMessage };
                    Logger.DebugFormat("Moving message : {0} to deadletter", message);

                    if (_options.Notifications != null)
                        await _options.Notifications.MovingMessageToDeadLetterAsync(moveToDeadLetterEvent);

                    if (!moveToDeadLetterEvent.Cancel)
                    {
                        await _options.Provider.MoveToDeadLetterAsync(message, "UnableToProcess", "Failed to process in reasonable attempts");
                        return true;
                    }
                }
                catch (Exception ex)
                {
                    Logger.WarnException("Moving message to deadletter failed for messageid {0} : ", ex, baseMessage.MessageId);
                    throw;
                }
            }
            return false;
        }

        private async Task ProccessMessageHandlingAsync(MessageType message, BaseMessage baseMessage, IMessageHandlerResolver resolver)
        {
            try
            {
                var processingTask = ProcessMessageAsync(baseMessage, resolver);

                //Loop until the task is completed;
                bool loop = true;
                var task = processingTask.ContinueWith((t) => { loop = false; });

                //Maximum time before throwing timeout expection.
                var timeout = _options.HandlerTimeOut ?? DefaultTimeOut;
                //Alternative it can be message based;
                if (_options.MessageBasedTimeOutProvider != null)
                    timeout = _options.MessageBasedTimeOutProvider(baseMessage) ?? timeout;

                var MaximumTimeTask = Task.Delay(timeout);
                while (loop)
                {

                    var t = await Task.WhenAny(task, Task.Delay(_options.AutoRenewLockTimerDuration ?? DefaultLockRenewTimer), MaximumTimeTask);
                    if (t == MaximumTimeTask)
                        throw new TimeoutException(string.Format("The handler could not finish in given time :{0}", timeout));

                    Logger.DebugFormat("Renewing Task<processingTask:{0}>", processingTask.Status.ToString());
                    try
                    {
                        await _options.Provider.RenewLockAsync(message);
                    }
                    catch (Exception ex)
                    {
                        Logger.InfoException("Renew Lock Exception: {0}", ex);
                    }
                }
                try
                {
                    await processingTask.ConfigureAwait(false); // Make it throw exception

                }
                catch (Exception ex)
                {
                    Logger.WarnException("Processing Execution failed for messageid {0} : ", ex, baseMessage.MessageId);
                    throw;

                }
            }
            catch (Exception ex)
            {
                Logger.WarnException("Main Execution Loop failed for messageid {0} : ", ex, baseMessage.MessageId);
                throw;
            }
        }

        private async Task FinalizeMessageAsync(MessageType message, BaseMessage baseMessage, TimeSpan transmitTime, Stopwatch sw, IMessageHandlerResolver resolver)
        {
            try
            {
                //Everything ok, so take it off the queue
                await _options.Provider.CompleteMessageAsync(message);

                sw.Stop();


                if (_options.Notifications != null)
                    await _options.Notifications.MessageCompletedAsync(
                        new MessageCompletedNotification(resolver)
                        {
                            Message = baseMessage,
                            Elapsed = sw.Elapsed,
                            ElapsedUntilReceived = transmitTime,
                            WorkingCount = _isWorking
                        });
            }
            catch (Exception ex)
            {
                Logger.WarnException("Notification MessageCompleted Failed for messageid {0} : ", ex, baseMessage.MessageId);
            }
        }

        private async Task ReEnQueueMessageAsyncIfNeeded(BaseMessage baseMessage,IMessageHandlerResolver resolver)
        {
            try
            {

                if (Attribute.IsDefined(baseMessage.GetType(), typeof(MessageScheduleAttribute)))
                {
                    var sender = resolver.GetHandler(typeof(IMessageProcessorClientProvider<MessageType>))
                        as IMessageProcessorClientProvider<MessageType>;
                    await sender.SendMessageAsync(baseMessage);

                }
            }
            catch (Exception ex)
            {
                Logger.WarnException("Failed to resend timed message {0} : ", ex, baseMessage.MessageId);
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
            _completeBlocker.Set(); //Runner shoul now complete.
            _options.Provider.Dispose();

        }

        public static TimeSpan DefaultTimeOut = TimeSpan.FromMinutes(10);
    }
}
