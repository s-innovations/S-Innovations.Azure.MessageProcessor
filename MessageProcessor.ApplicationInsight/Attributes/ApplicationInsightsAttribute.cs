using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.ApplicationInsights.Attributes
{
    public class ApplicationInsightsAttribute : Attribute
    {
        public Type EventTelemetryMetadataProvider { get; set; }
        public string PropertyName { get; set; }
        public string EventTelemetryName { get; set; }
        public ApplicationInsightsAttribute()
        {

        }
    }
}
