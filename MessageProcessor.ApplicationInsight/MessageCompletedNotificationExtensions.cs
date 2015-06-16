
using Microsoft.ApplicationInsights.DataContracts;
using SInnovations.Azure.MessageProcessor.ApplicationInsights;
using SInnovations.Azure.MessageProcessor.ApplicationInsights.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.Core.Notifications
{
    public static class MessageCompletedNotificationExtensions
    {

        public static async Task<EventTelemetry> CreateEventTelemetryAsync(this MessageCompletedNotification notice, string name = "MessageCompleted")
        {
            var t = new EventTelemetry(name)
            {
                Timestamp = DateTimeOffset.Now, 
            };
            t.Properties.Add("MessageId", notice.Message.MessageId);
            t.Properties.Add("MessageType", notice.Message.GetType().Name);
            t.Metrics.Add("Elapsed", notice.Elapsed.TotalMilliseconds);
           
   

            var props = notice.Message.GetType().GetProperties().Where(
                prop => Attribute.IsDefined(prop, typeof(ApplicationInsightsAttribute)));

            foreach (var prop in props)
            {
                ApplicationInsightsAttribute attr = (ApplicationInsightsAttribute)prop.GetCustomAttributes(typeof(ApplicationInsightsAttribute), true)[0];

                if (attr.EventTelemetryMetadataProvider != null)
                {
                    var provider = notice.Resolver.GetHandler(attr.EventTelemetryMetadataProvider) as IEventTelemetryMetadataProvider;

                    await provider.AddMetadataAsync(t, attr.PropertyTypeName ?? prop.Name, prop.GetValue(notice.Message));

                }
                else
                {
                    t.Properties.Add(attr.PropertyTypeName ?? prop.Name, prop.GetValue(notice.Message).ToString());
                }
            }


           

            return t;
        }
    }
}
