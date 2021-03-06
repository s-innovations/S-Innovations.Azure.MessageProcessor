﻿using SInnovations.Azure.MessageProcessor.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.AzureHandlers.Messages
{
    public class AzureSubscriptionBaseMessage : BaseMessage
    {

        public string AzureSubscriptionCertificateThumbprint { get; set; }

        public string AzureSubscriptionId { get; set; }

        public string AzureSubscriptionToken { get; set; }
    }
}
