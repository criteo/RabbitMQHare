using System;
using RabbitMQ.Client.Events;

namespace RabbitMQHare
{
    public interface IConsumer : IDisposable
    {
        /// <summary>
        /// Event handler for messages. If you modify this after Start methed is called, it won't be applied
        /// until next restart (=connection issue)
        ///  If you forgot to set this one, the consumer will swallow messages as fast as it can
        /// </summary>
        event BasicDeliverEventHandler MessageHandler;

        /// <summary>
        /// Event handler for messages handler failure. If you modify this after Start method is called, it won't be applied
        /// until next restart (connection issue). If this throws an error, you are screwed, buddy. Don't tempt the devil !
        /// Handler that is called when :
        /// 1)the messageHandler throws an exception
        /// 2)the consumer itself throws an exception.
        /// You have to decide wether to ack the message in both case (even if AcknowledgeMessageForMe is set to true)
        /// </summary>
        event ThreadedConsumer.CallbackExceptionEventHandlerWithMessage ErrorHandler;

        /// <summary>
        /// Handler called at each start (and restart). If you modify this after Start method is called, it won't be applied
        /// until next restart (connection issue). If this throws an error, you are screwed, buddy. Don't tempt the devil !
        /// </summary>
        event ConsumerEventHandler StartHandler;

        /// <summary>
        /// Handler called at each stop. If you modify this after Start method is called, it won't be applied
        /// until next restart (connection issue). If this throws an error, you are screwed, buddy. Don't tempt the devil !
        /// </summary>
        event ConsumerEventHandler StopHandler;

        /// <summary>
        /// Start consuming messages. This method is not thread safe.
        /// </summary>
        /// <param name="maxConnectionRetry">number of allowed retries before giving up</param>
        /// <returns>true if connection has succeeded</returns>
        bool Start(int maxConnectionRetry);

        ConsumerShutdownEventHandler GetShutdownHandler();
        ConsumerEventHandler GetDeleteHandler();

        /// <summary>
        /// Called when an exception is thrown when connecting to rabbit. It is called at most [MaxConnectionRetry] times before a more serious BrokerUnreachableException is thrown
        /// </summary>
        event RabbitConnectorCommon.TemporaryConnectionFailure TemporaryConnectionFailureHandler;

        /// <summary>
        /// Called when too many exceptions ([MaxConnectionRetry]) are thrown when connecting to rabbit.
        /// The object (Consumer or Publisher) will stop trying to connect and can be considered dead when this event is called
        /// </summary>
        event RabbitConnectorCommon.PermanentConnectionFailure PermanentConnectionFailureHandler;

        /// <summary>
        /// Called when a ACL exception is thrown.
        /// </summary>
        event RabbitConnectorCommon.ACLFailure ACLFailureHandler;

        /// <summary>
        /// Called when an exception is thrown by another handler, this obviously must not throw an exception (it will crash)
        /// </summary>
        event RabbitConnectorCommon.EventHandlerFailure EventHandlerFailureHandler;
    }
}