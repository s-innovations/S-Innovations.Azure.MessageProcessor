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
