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
    /// <summary>
    /// NOT TESTED
    /// </summary>
    public class UnityHandlerResolver : IMessageHandlerResolver
    {
        private IUnityContainer Container;
    

        public UnityHandlerResolver(params Assembly[] assemblies)
        {
            ConfigureUnity(assemblies);
        }
        public UnityHandlerResolver(IUnityContainer container)
        {
            Container = container.CreateChildContainer();
        }
        public object GetHandler(Type constructed)
        {

            return Container.Resolve(constructed);

        }

        private void ConfigureUnity(IEnumerable<Assembly> assemblies)
        {
            var kernel = new UnityContainer();
            Type handlerType = typeof(IMessageHandler<>);
            foreach(var asm in assemblies)
            {
                foreach(var type in asm.GetTypes().Where(t=>typeof(IMessageHandler<>).IsAssignableFrom(t)))
                {
                    foreach (var contract in type.GetInterfaces())
                    {
                        
                        if (contract.GenericTypeArguments.Length > 0 && contract.GetGenericTypeDefinition().Equals(handlerType))
                        {
                            Type[] typeArgs = { contract.GenericTypeArguments[0] };
                            Type constructed = handlerType.MakeGenericType(typeArgs);
                            kernel.RegisterType(constructed, type);
                        }
                    }
                }
            }

            Container = kernel;
        }

        public void Dispose()
        {
            Container.Dispose();
        }
    }
}
