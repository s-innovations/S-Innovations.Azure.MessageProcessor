using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.ApplicationInsights
{
    public interface IEventTelemetryMetadataProvider
    {

        Task AddMetadataAsync(Microsoft.ApplicationInsights.DataContracts.EventTelemetry t);
    }
}
