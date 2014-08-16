using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.Core.Notifications
{
    public class DefaultNotifications : IMessageProcessorNotifications
    {
        public Func<MovingToDeadLetterNotification, Task> OnMovingMessageToDeadLetter { get; set; }
        public Func<MessageCompletedNotification, Task> OnMessageCompleted { get; set; }
        public Func<HandlerNotFoundNotification, Task> OnHandlerNotFoundNotification { get; set; }
        public Func<IdleRunningNotification, Task> OnIdleNotification { get; set; }

        public Task MovingMessageToDeadLetterAsync(MovingToDeadLetterNotification moveToDeadLetterEvent)
        {
            if (OnMovingMessageToDeadLetter != null)
                return OnMovingMessageToDeadLetter(moveToDeadLetterEvent);
            return Task.FromResult(0);
        }

        public Task MessageCompletedAsync(MessageCompletedNotification messageCompletedNotification)
        {
            if (OnMessageCompleted != null)
                return OnMessageCompleted(messageCompletedNotification);
            return Task.FromResult(0);
        }

        public Task HandlerWasNotFoundAsync(HandlerNotFoundNotification handlerNotFoundNotification)
        {
            if (OnHandlerNotFoundNotification != null)
                return OnHandlerNotFoundNotification(handlerNotFoundNotification);
            return Task.FromResult(0);
        }


        public Task RunningIdleAsync(IdleRunningNotification idleRunningNotification)
        {
            if (OnIdleNotification != null)
                return OnIdleNotification(idleRunningNotification);
            return Task.FromResult(0);
        }
    }
}
