using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using SInnovations.Azure.MessageProcessor.Core;
using SInnovations.Azure.MessageProcessor.Core.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MessageProcessor.BlobStorageRepository
{
    public class BlobRepository : IModelRepository
    {
        CloudBlobContainer container;

        public BlobRepository(CloudBlobContainer container)
        {
            this.container = container;
        }

        public async Task SaveModelAsync(IModelBasedMessage message)
        {
            using (var compressionStream = new GZipStream(await this.container.GetBlockBlobReference(message.MessageId).OpenWriteAsync(), CompressionMode.Compress))
            {
                using (var jsonWriter = new JsonTextWriter(new StreamWriter(compressionStream)))
                {
                    JsonSerializer s = JsonSerializer.CreateDefault();
                    s.Serialize(jsonWriter,message.GetAndDeleteModel());

                }

            }
            //  var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(model));
            //    var compressedStream = new MemoryStream();
            //    using (var compressionStream = new GZipStream(compressedStream, CompressionMode.Compress))
            //    {
            //        await compressionStream.WriteAsync(bytes, 0, bytes.Length);

            //    }
            //    Data = compressedStream.ToArray();
        }

        public async Task GetModelAsync(IModelBasedMessage message)
        {
           // var decompressedStream = new MemoryStream();
            using (var compressionStream = new GZipStream(await this.container.GetBlockBlobReference(message.MessageId).OpenReadAsync(), CompressionMode.Decompress))
            {
               // await compressionStream.CopyToAsync(decompressedStream);
                using(var jsonReader = new JsonTextReader(new StreamReader(compressionStream)))
                {
                    JsonSerializer s = JsonSerializer.CreateDefault();
                    message.SetModel(s.Deserialize(jsonReader, message.GetModelType()));

                }
            }
        }
    }
    public class StorageAccountProvider : IModelRepositoryProvider
    {
        CloudBlobContainer container;

        public StorageAccountProvider(CloudBlobContainer container)
        {
            this.container = container;
        }
        public IModelRepository GetRepository()
        {
            return new BlobRepository(this.container);
        }
    }
}
