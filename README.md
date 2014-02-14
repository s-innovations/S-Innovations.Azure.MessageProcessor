
Azure Webrole Message Processor
============================
[![GoogleAnalyticsTracker Nightly Build Status](https://www.myget.org/BuildSource/Badge/s-innovations?identifier=d21c651e-1853-4a8d-8b4c-8e1f724ef3f0)](https://www.myget.org/gallery/googleanalyticstracker)

I created this project with the boiler plate code I always use when doing a worker/web role for queue processing. 

It factor out the queue provider such both the servicebus and storage queue can be pluged into the message processor.

Further more it supports processing for multiply message types in the queue where you just have to implement the IMessageHandler interface for the message you create. And it support Async Processors.

```
public interface IMessageHandler<MessageType>  where MessageType : BaseMessage
{
    Task HandleAsync(MessageType T);
}

```



