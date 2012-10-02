using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RMQClient
{
    public class ThreadedConsumer : DefaultBasicConsumer
    {
        private Thread _dispatch;
        private TaskScheduler _scheduler;
        private CancellationTokenSource _cts;
        private Queue<BasicDeliverEventArgs> _queue;
        private bool _queueClosed;
        private int _taskCount;

        public ushort MaxWorker { get; private set; }
        public bool AutoAck { get; set; }
        public int ShutdownTimeout { get; set; }

        public event BasicDeliverEventHandler OnMessage;
        public event ConsumerEventHandler OnStart;
        public event ConsumerEventHandler OnStop;
        public event ConsumerEventHandler OnDelete;
        public event CallbackExceptionEventHandler OnError;
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
                    try
                    {
                        BasicDeliverEventArgs e;
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
                                if (OnError != null) OnError(this, new CallbackExceptionEventArgs(ex));
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
                        if (OnError != null) OnError(this, new CallbackExceptionEventArgs(ex));
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
            BasicDeliverEventArgs e = new BasicDeliverEventArgs();
            e.ConsumerTag = consumerTag;
            e.DeliveryTag = deliveryTag;
            e.Redelivered = redelivered;
            e.Exchange = exchange;
            e.RoutingKey = routingKey;
            e.BasicProperties = properties;
            e.Body = body;

            lock (_queue)
            {
                if (_queueClosed) return;
                _queue.Enqueue(e);
                Monitor.Pulse(_queue);
            }
        }
    }
}
