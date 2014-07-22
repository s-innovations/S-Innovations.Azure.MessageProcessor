using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebrole.MessageProcessor.Core
{
    [Serializable]
    public abstract class BaseMessage
    {
        /// <summary>
        /// A message ID for the current message.
        /// </summary>
        public string MessageId { get; internal set; }
        
        /// <summary>
        /// Used when provider dont support serialization
        /// </summary>
        /// <returns></returns>
        public byte[] ToBinary()
        {
            BinaryFormatter bf = new BinaryFormatter();
            byte[] output = null;
            using (MemoryStream ms = new MemoryStream())
            {
                ms.Position = 0;
                bf.Serialize(ms, this);
                output = ms.GetBuffer();
            }
            return output;
        }

     

    }
   
}
