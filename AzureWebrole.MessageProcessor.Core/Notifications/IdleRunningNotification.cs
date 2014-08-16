using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.Core.Notifications
{
    public class IdleRunningNotification
    {
        public TimeSpan IdleTime { get; set; }

      
        internal TaskCompletionSource<int> RestartTask;

        /// <summary>
        /// Bring the client to stop when done, waiting until next message completes if any.
        /// </summary>
        public Task RestartClientAsync()
        {
            RestartTask = new TaskCompletionSource<int>();
            return RestartTask.Task;
        }
    }
}
