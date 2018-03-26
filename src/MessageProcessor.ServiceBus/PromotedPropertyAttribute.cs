using SInnovations.Azure.MessageProcessor.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.ServiceBus
{
    public class PromotedPropertyAttribute : Attribute
    {
        public PromotedPropertyAttribute()
        {

        }

        public string Name { get; set; }
    }

  
}
