using AzureWebrole.MessageProcessor.Core;
using Microsoft.Practices.Unity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace AzureWebRole.MessageProcessor.Unity
{
    public class UnityHandlerResolver : MessageHandlerResolver
    {
        private IUnityContainer Container;

        public UnityHandlerResolver(params Assembly[] assemblies)
        {
            ConfigureUnity(assemblies);
        }
        public object GetHandler(Type constructed)
        {
            throw new NotImplementedException();
        }

        private void ConfigureUnity(IEnumerable<Assembly> assemblies)
        {
            var kernel = new UnityContainer();
            foreach(var asm in assemblies)
            {
                foreach(var type in asm.GetTypes().Where(t=>typeof(IMessageHandler<>).IsAssignableFrom(t)))
                {
                    Type handlerType = typeof(IMessageHandler<>);
                    Type[] typeArgs = { type.GenericTypeArguments[0] };
                    Type constructed = handlerType.MakeGenericType(typeArgs);
                    kernel.RegisterType(constructed,type);
                }
            }
;
            Container = kernel;
        }
    }
}
