using System;
using System.Threading;
using RabbitMQ.Client;

namespace RabbitMQHare
{
    public interface IPublisher : IDisposable
    {

        /// <summary>
        /// Called when queue is full and message are not enqueued.
        /// </summary>
        event RabbitPublisher.MessageNotEnqueued MessageNotEnqueuedHandler;

        /// <summary>
        /// Handler called at each start (and restart)
        /// This is different from StartHandler of consumer, so you can modify this at any time
        /// </summary>
        event RabbitPublisher.StartHandler OnStart;

        /// <summary>
        /// Handler called when some messages are not acknowledged after being published.
        /// It can happen if rabbitmq send a nacked message, or if the connection is lost
        /// before ack are received.
        /// Using this handler has no sense when UseConfirms setting is false.
        /// </summary>
        event RabbitPublisher.NAckedHandler OnNAcked;

        /// <summary>
        /// The settings of the publisher
        /// </summary>
        HarePublisherSettings MySettings { get; }

        /// <summary>
        /// This is true if the publisher has been sucessfully started once.
        /// </summary>
        bool Started { get; }

        /// <summary>
        /// Start to publish messages. This method is usually not thread-safe.
        /// </summary>
        /// <param name="maxConnectionRetry"></param>
        /// <returns>true if the connection has succeeded. If false, you can retry to connect.</returns>
        bool Start(int maxConnectionRetry);

        /// <summary>
        /// Add message that will be sent asynchronously. This method is thread-safe
        /// </summary>
        /// <param name="routingKey">routing key used to route the message. If not needed just put "toto"</param>
        /// <param name="message">the message you want to send</param>
        /// <param name="messageProperties">properties of the message. If set, overrides ConstructProperties parameter</param>
        /// <returns>false if the message was droppped instead of added to the queue</returns>
        bool Publish(string routingKey, byte[] message, IBasicProperties messageProperties);

        /// <summary>
        /// Add message that will be sent asynchronously. This method is thread-safe
        /// </summary>
        /// <param name="routingKey">routing key used to route the message. If not needed just put "toto"</param>
        /// <param name="message">the message you want to send</param>
        /// <returns>false if the message was droppped instead of added to the queue</returns>
        bool Publish(string routingKey, byte[] message);

        /// <summary>
        /// Add message that will be sent asynchronously BUT might block if queue is full. This method is thread-safe
        /// </summary>
        /// <param name="routingKey">routing key used to route the message. If not needed just put "toto"</param>
        /// <param name="message">the message you want to send</param>
        /// <param name="messageProperties">properties of the message. If set, overrides ConstructProperties parameter</param>
        void BlockingPublish(string routingKey, byte[] message, IBasicProperties messageProperties);

        /// <summary>
        /// Add message that will be sent asynchronously BUT might block if queue is full. This method is thread-safe
        /// </summary>
        /// <param name="routingKey">routing key used to route the message. If not needed just put "toto"</param>
        /// <param name="message">the message you want to send</param>
        void BlockingPublish(string routingKey, byte[] message);

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

        /// <summary>
        /// Returns #messages waiting in the destination queue.
        /// It makes no sense to call this when sending messages to an exchange (it will throw an exception)
        /// This will also throw an exception when the connection is broken.
        /// Calling MessageCount can be useful to build some kind of flow control by polling it.
        /// </summary>
        uint MessageCount { get; }

        /// <summary>
        /// Trigger stop process. As soon as this method is called, message enqueing may trigger exceptions.
        /// It is the user responsability not to call any Publish method (directly, through an event handler, ...).
        /// When the returned mutex is set, all messages have been sent.
        /// </summary>
        /// <returns>A mutex to wait on for stop process completion.</returns>
        ManualResetEventSlim Stop();

        /// <summary>
        /// Blocking method to wait for stop process completion. At the end, this publisher is disposable.
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns>true if stop process completed successfully, false otherwise. In the latter case, messages have probably been lost</returns>
        bool GracefulStop(TimeSpan timeout);
    }
}