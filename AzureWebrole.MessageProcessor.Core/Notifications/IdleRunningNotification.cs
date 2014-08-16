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



        private IMessageProcessorClient messageProcessorClient;

        public IdleRunningNotification(IMessageProcessorClient messageProcessorClient)
        {
            // TODO: Complete member initialization
            this.messageProcessorClient = messageProcessorClient;
        }

        /// <summary>
        /// Bring the client to stop when done, waiting until next message completes if any.
        /// </summary>
        public Task RestartClientAsync()
        {
           
            return this.messageProcessorClient.RestartProcessorAsync();
            
        }
    }
}
