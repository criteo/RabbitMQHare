using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQHare
{
    public class ThreadedConsumer : DefaultBasicConsumer
    {
        /// <summary>
        /// Callback used to give control on the message being processed while an exception was thrown.
        /// </summary>
        /// <param name="sender">The threadedConsumer</param>
        /// <param name="exception">The exception that was thrown. Might be an business exception (thrown by your OnMessage handler) or internal exception thrown by consumer</param>
        /// <param name="e">The message so you can ack it if needed.</param>
        public delegate  void CallbackExceptionEventHandlerWithMessage(object sender, CallbackExceptionEventArgs exception, BasicDeliverEventArgs e);

        private readonly Thread _dispatch;
        private readonly TaskScheduler _scheduler;
        private readonly CancellationTokenSource _cts;
        private readonly Queue<BasicDeliverEventArgs> _queue;
        private bool _queueClosed;
        private int _taskCount;

        /// <summary>
        ///  Maxmimum number of concurrents messages processed at the same time.
        /// </summary>
        public ushort MaxWorker { get; private set; }
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


        public ThreadedConsumer(IModel model, ushort maxWorker, bool autoAck)
            : this(model, maxWorker, autoAck, TaskScheduler.Default)
        {
        }

        public ThreadedConsumer(IModel model, ushort maxWorker, bool autoAck, TaskScheduler scheduler)
            : base(model)
        {
            _scheduler = scheduler;
            _cts = new CancellationTokenSource();
            _queue = new Queue<BasicDeliverEventArgs>();
            _queueClosed = false;
            _taskCount = 0;

            AutoAck = autoAck;
            ShutdownTimeout = Timeout.Infinite;
            MaxWorker = Math.Min(maxWorker, (ushort)scheduler.MaximumConcurrencyLevel);
            Model.BasicQos(0, MaxWorker, false);

            _dispatch = new Thread(() =>
            {
                while (!_cts.IsCancellationRequested)
                {
                    BasicDeliverEventArgs e = null;
                    try
                    {
                        lock (_queue)
                        {
                            while (!_cts.IsCancellationRequested && (_queue.Count == 0 || _taskCount == MaxWorker))
                            {
                                Monitor.Wait(_queue);
                            }
                            if (_queueClosed) break;
                            e = _queue.Dequeue();
                            ++_taskCount;
                        }

                        var task = new Task(() =>
                        {
                            try
                            {
                                if (OnMessage != null) OnMessage(this, e);
                                if (AutoAck) Model.BasicAck(e.DeliveryTag, false);
                            }
                            catch (Exception ex)
                            {
                                if (OnError != null) OnError(this, new CallbackExceptionEventArgs(ex),e);
                            }
                            finally
                            {
                                lock (_queue)
                                {
                                    --_taskCount;
                                    Monitor.Pulse(_queue);
                                }
                            }
                        }, _cts.Token);

                        task.Start(_scheduler);
                    }
                    catch (Exception ex)
                    {
                        if (OnError != null) OnError(this, new CallbackExceptionEventArgs(ex), e);
                    }
                }

                lock (_queue)
                {
                    while (_taskCount > 0) Monitor.Wait(_queue);
                }
            });
            _dispatch.IsBackground = true;
        }

        public override void OnCancel()
        {
            base.OnCancel();

            _cts.Cancel();
            lock (_queue)
            {
                _queueClosed = true;
                Monitor.PulseAll(_queue);
            }
            _dispatch.Join(ShutdownTimeout);
        }

        public override void HandleBasicConsumeOk(string consumerTag)
        {
            base.HandleBasicConsumeOk(consumerTag);
            if (OnStart != null) OnStart(this, new ConsumerEventArgs(consumerTag));
            _dispatch.Start();
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

            lock (_queue)
            {
                if (_queueClosed) return;
                _queue.Enqueue(e);
                Monitor.Pulse(_queue);
            }
        }
    }
}
