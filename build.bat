@echo Off
set config=%1
if "%config%" == "" (
   set config=Release
)

set version=
if not "%PackageVersion%" == "" (
   set version=-Version %PackageVersion%
)

REM Build
%WINDIR%\Microsoft.NET\Framework\v4.0.30319\msbuild S-Innovations.Azure.MessageProcessor.sln /p:Configuration="%config%" /m /v:M /fl /flp:LogFile=msbuild.log;Verbosity=Normal /nr:false

REM Package
mkdir Build
cmd /c %nuget% pack "AzureWebRole.MessageProcessor.AzureHandlers\MessageProcessor.AzureHandlers.csproj" -IncludeReferencedProjects -o Build -p Configuration=%config% 
cmd /c %nuget% pack "AzureWebrole.MessageProcessor.Core\MessageProcessor.Core.csproj" -IncludeReferencedProjects -o Build -p Configuration=%config% 
cmd /c %nuget% pack "AzureWebrole.MessageProcessor.ServiceBus\MessageProcessor.ServiceBus.csproj" -IncludeReferencedProjects -o Build -p Configuration=%config% 
cmd /c %nuget% pack "AzureWebRole.MessageProcessor.Unity\MessageProcessor.Unity.csproj" -IncludeReferencedProjects -o Build -p Configuration=%config%
cmd /c %nuget% pack "MessageProcessor.BlobStorageRepository\MessageProcessor.BlobStorageRepository.csproj" -IncludeReferencedProjects -o Build -p Configuration=%config% 