/* Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
*/
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQHare
{
    /// <summary>
    /// Underlying class for consuming messages
    /// </summary>
    public class ThreadedConsumer : BaseConsumer
    {
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

        public ThreadedConsumer(IModel model, ushort maxWorker, bool autoAck, int prefetchCount)
            : this(model, maxWorker, autoAck, TaskScheduler.Default, prefetchCount)
        {
        }

        public ThreadedConsumer(IModel model, ushort maxWorker, bool autoAck, TaskScheduler scheduler, int prefetchCount)
            : base(model, autoAck)
        {
            _scheduler = scheduler;
            _cts = new CancellationTokenSource();
            _queue = new Queue<BasicDeliverEventArgs>();
            _queueClosed = false;
            _taskCount = 0;

            MaxWorker = Math.Min(maxWorker, (ushort)scheduler.MaximumConcurrencyLevel);
            Model.BasicQos(0, (ushort)prefetchCount, false);

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

                        CreateAndStartTask(e);
                    }
                    catch (Exception ex)
                    {
                        DispatchError(e, ex);
                    }
                }

                lock (_queue)
                {
                    while (_taskCount > 0) Monitor.Wait(_queue);
                }
            }) {IsBackground = true};
        }

        private void CreateAndStartTask(BasicDeliverEventArgs e)
        {
            var task = new Task(() =>
            {
                try
                {
                    DispatchMessage(e);
                    if (AutoAck) Model.BasicAck(e.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    DispatchError(e, ex);
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
            _dispatch.Start();
        }

        protected override void ProcessOne(BasicDeliverEventArgs e)
        {
            lock (_queue)
            {
                if (_queueClosed) return;
                _queue.Enqueue(e);
                Monitor.Pulse(_queue);
            }
        }
    }
}
