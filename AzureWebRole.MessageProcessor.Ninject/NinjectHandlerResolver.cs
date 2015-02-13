using Ninject;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Ninject.Extensions.Conventions;
using System.Reflection;
using SInnovations.Azure.MessageProcessor.Core;

namespace SInnovations.Azure.MessageProcessor.Ninject
{
    public class NinjectHandlerResolver : IMessageHandlerResolver
    {
        private IKernel NinjectKernel;

        public NinjectHandlerResolver(params Assembly[] assemblies)
        {
            ConfigureNinject(assemblies);
        }
        public object GetHandler(Type constructed)
        {
            return NinjectKernel.Get(constructed);
        }

        private void ConfigureNinject(IEnumerable<Assembly> assemblies)
        {
            var kernel = new StandardKernel();
            kernel.Bind(x => x.From(assemblies)
                  .SelectAllClasses().InheritedFrom(typeof(IMessageHandler<>))
                  .BindAllInterfaces());
            NinjectKernel = kernel;
        }

        public void Dispose()
        {
           
        }
    }
}
