using Microsoft.ApplicationInsights.DataContracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.ApplicationInsights
{
    public interface IEventTelemetryMetadataProvider
    {

        Task AddMetadataAsync(EventTelemetry t, string propertyName, object PropertyValue);
    }
}
