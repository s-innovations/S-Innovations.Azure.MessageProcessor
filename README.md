
Azure Webrole Message Processor
============================
![](https://sinnovations.visualstudio.com/DefaultCollection/_apis/public/build/definitions/40c16cc5-bf99-47d4-a814-56c38cc0ea24/7/badge)

I created this project with the boiler plate code I always use when doing a worker/web role for queue processing. 

It factor out the queue provider such both the servicebus and storage queue can be pluged into the message processor.

Further more it supports processing for multiply message types in the queue where you just have to implement the IMessageHandler interface for the message you create. And it support Async Processors.

```
public interface IMessageHandler<MessageType>  where MessageType : BaseMessage
{
    Task HandleAsync(MessageType T);
}

```



