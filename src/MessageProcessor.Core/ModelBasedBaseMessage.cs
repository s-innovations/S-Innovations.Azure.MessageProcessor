using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.MessageProcessor.Core
{
    public interface IModelBasedMessage : IBaseMessage
    {
        Object GetAndDeleteModel();
        Type GetModelType();
        void SetModel(object model);
    }

    public class ModelBasedBaseMessage<T> : BaseMessage, IModelBasedMessage
    {


        
        public T Model { get; set; }

        //public async Task<T> GetModelAsync()
        //{
        //    var decompressedStream = new MemoryStream();
        //    using (var compressionStream = new GZipStream(new MemoryStream(Data), CompressionMode.Decompress))
        //    {
        //        await compressionStream.CopyToAsync(decompressedStream);
        //    }
        //    decompressedStream.Seek(0, SeekOrigin.Begin);
        //    return JsonConvert.DeserializeObject<TilesCombineModel>(Encoding.UTF8.GetString(decompressedStream.ToArray()));
        //}
        //public async Task SetModelAsync(T model)
        //{
        //    var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(model));
        //    var compressedStream = new MemoryStream();
        //    using (var compressionStream = new GZipStream(compressedStream, CompressionMode.Compress))
        //    {
        //        await compressionStream.WriteAsync(bytes, 0, bytes.Length);

        //    }
        //    Data = compressedStream.ToArray();
        //    //using (MD5 md5 = System.Security.Cryptography.MD5.Create())
        //    //{
        //    //    var hash = md5.ComputeHash(Data);
        //    //    StringBuilder sb = new StringBuilder();
        //    //    for (int i = 0; i < hash.Length; i++)
        //    //    {
        //    //        sb.Append(hash[i].ToString("X2"));
        //    //    }
        //    //    this.MessageId = sb.ToString();
        //    //}
        //}

        public object GetAndDeleteModel()
        {
            var model = Model;
            Model = default(T);
            return model;
        }


        public Type GetModelType()
        {
            return typeof(T);
        }


        public void SetModel(object model)
        {
            Model = (T)model;
        }
    }
}
