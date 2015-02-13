using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.Core.Notifications
{
    public interface IMessageProcessorNotifications
    {

        Task MovingMessageToDeadLetterAsync(MovingToDeadLetterNotification moveToDeadLetterEvent);
        Task MessageCompletedAsync(MessageCompletedNotification messageCompletedNotification);

        Task HandlerWasNotFoundAsync(HandlerNotFoundNotification handlerNotFoundNotification);
        Task RunningIdleAsync(IdleRunningNotification idleRunningNotification);
    }
}
