using AzureWebrole.MessageProcessor.Core;
using Ninject;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Ninject.Extensions.Conventions;
using System.Reflection;

namespace AzureWebRole.MessageProcessor.Ninject
{
    public class NinjectHandlerResolver : MessageHandlerResolver
    {
        private IKernel NinjectKernel;

        public NinjectHandlerResolver(params Assembly[] assemblies)
        {
            ConfigureNinject(assemblies);
        }
        public object GetHandler(Type constructed)
        {
            throw new NotImplementedException();
        }

        private void ConfigureNinject(IEnumerable<Assembly> assemblies)
        {
            var kernel = new StandardKernel();
            kernel.Bind(x => x.From(assemblies)
                  .SelectAllClasses().InheritedFrom(typeof(IMessageHandler<>))
                  .BindAllInterfaces());
            NinjectKernel = kernel;
        }
    }
}
