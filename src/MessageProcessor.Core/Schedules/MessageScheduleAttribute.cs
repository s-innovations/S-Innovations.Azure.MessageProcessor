using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace SInnovations.Azure.MessageProcessor.Core.Schedules
{
    public static class ScheduleExtensions
    {
        public static DateTimeOffset Trim(this DateTimeOffset date, long roundTicks)
        {
            return new DateTimeOffset(date.Ticks - date.Ticks % roundTicks, date.Offset);
        }

    }
    public class MessageScheduleAttribute : Attribute
    {
        public String StartTime { get; set; }
        public String EndTime { get; set; }

        public String TimeBetweenSchedules { get; set; }

        public TimeSpan Period { get { return XmlConvert.ToTimeSpan(TimeBetweenSchedules); } }
        public DateTimeOffset Start { get { return XmlConvert.ToDateTimeOffset(StartTime); } }


        public DateTime GetNextWindowTime()
        {
    
            return (DateTimeOffset.UtcNow.Trim(Period.Ticks) + Period).UtcDateTime;
        }
    }
}
