using System;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQHare
{
    /// <summary>
    /// Underlying class for consuming messages
    /// </summary>
    public abstract class BaseConsumer : DefaultBasicConsumer
    {
        /// <summary>
        /// Callback used to give control on the message being processed while an exception was thrown.
        /// </summary>
        /// <param name="sender">The threadedConsumer</param>
        /// <param name="exception">The exception that was thrown. Might be an business exception (thrown by your OnMessage handler) or internal exception thrown by consumer</param>
        /// <param name="e">The message so you can ack it if needed.</param>
        public delegate void CallbackExceptionEventHandlerWithMessage(object sender, CallbackExceptionEventArgs exception, BasicDeliverEventArgs e);

        /// <summary>
        /// If true, threadedConsumer will ack messages right after the synchronous call of OnMessage. Else you have to ack messages.
        /// </summary>
        public bool AutoAck { get; set; }

        /// <summary>
        /// Maximum waiting time of OnMessage handlers to finish before disposing. Default to Infinite.
        /// </summary>
        public int ShutdownTimeout { get; set; }

        /// <summary>
        /// Handler called when a message is received and a spot is freed. If not provided it will swallow messages as fast as it can.
        /// </summary>
        public event BasicDeliverEventHandler OnMessage;
        /// <summary>
        /// Handler called at start
        /// </summary>
        public event ConsumerEventHandler OnStart;
        /// <summary>
        /// Handler called when the consumer will stop
        /// </summary>
        public event ConsumerEventHandler OnStop;
        /// <summary>
        /// Handler called when rabbit mq broker deletes the connection
        /// </summary>
        public event ConsumerEventHandler OnDelete;
        /// <summary>
        /// Handler called for all errors other than connection. It is your responsability to ack the message that may be passed.
        /// </summary>
        public event CallbackExceptionEventHandlerWithMessage OnError;
        /// <summary>
        /// Handler called when connection issues occurs.
        /// </summary>
        public event ConsumerShutdownEventHandler OnShutdown;

        protected BaseConsumer(IModel model, bool autoAck)
            : base(model)
        {
            AutoAck = autoAck;
            ShutdownTimeout = Timeout.Infinite;
        }

        public override void HandleBasicConsumeOk(string consumerTag)
        {
            base.HandleBasicConsumeOk(consumerTag);
            if (OnStart != null) OnStart(this, new ConsumerEventArgs(consumerTag));
        }

        public override void HandleBasicCancelOk(string consumerTag)
        {
            base.HandleBasicCancelOk(consumerTag);
            if (OnStop != null) OnStop(this, new ConsumerEventArgs(consumerTag));
        }

        public override void HandleBasicCancel(string consumerTag)
        {
            base.HandleBasicCancel(consumerTag);
            if (OnDelete != null) OnDelete(this, new ConsumerEventArgs(consumerTag));
        }

        public override void HandleModelShutdown(IModel model, ShutdownEventArgs reason)
        {
            base.HandleModelShutdown(model, reason);
            if (OnShutdown != null) OnShutdown(this, reason);
        }

        public override void HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IBasicProperties properties, byte[] body)
        {
            var e = new BasicDeliverEventArgs
                {
                    ConsumerTag = consumerTag,
                    DeliveryTag = deliveryTag,
                    Redelivered = redelivered,
                    Exchange = exchange,
                    RoutingKey = routingKey,
                    BasicProperties = properties,
                    Body = body
                };
            ProcessOne(e);
        }

        protected void DispatchError(BasicDeliverEventArgs e, Exception exception)
        {
            if (OnError != null) OnError(this, new CallbackExceptionEventArgs(exception), e);
        }

        protected void DispatchMessage(BasicDeliverEventArgs e)
        {
            if (OnMessage != null) OnMessage(this, e);
        }

        protected abstract void ProcessOne(BasicDeliverEventArgs e);
    }
}
