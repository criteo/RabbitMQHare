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
﻿using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQHare
{
    /// <summary>
    /// Settings used to construct a consumer. If you don't use HareConsumerSettings.DefaultSettings you have to fill all parameters.
    /// </summary>
    public struct HareConsumerSettings : IHareSettings
    {
        /// <summary>
        /// Maxmimum number of concurrents messages processed at the same time. DefaultSettings sets it to 1
        /// </summary>
        public int MaxWorkers { get; set; }

        /// <summary>
        /// If false, it is your responsability to ack each message in your MessageHandler. DefaultSettings sets it to True
        /// </summary>
        public bool AcknowledgeMessageForMe { get; set; }

        /// <summary>
        /// You can provide a way to modify the IBasicProperties of all messages. DefaultSettings sets it to  "text/plain" type and transient messages
        /// </summary>
        public ConnectionFactory ConnectionFactory { get; set; }

        /// <summary>
        /// When connection fails, indicates the maximum number of tries before calling the permanent connection failure handler. DefaultSettings sets it to 5. -1 is infinite
        /// </summary>
        public int MaxConnectionRetry { get; set; }

        /// <summary>
        /// Interval between two retries when connecting to rabbitmq. Default is 5 seconds
        /// </summary>
        public TimeSpan IntervalConnectionTries { get; set; }

        /// <summary>
        /// Time given to message handler to finish in case of cancel event. Infinite if not set
        /// </summary>
        public TimeSpan? CancelationTime { get; set; }

        /// <summary>
        /// Task scheduler used by workers
        /// </summary>
        public TaskScheduler TaskScheduler { get; set; }

        /// <summary>
        /// When false (the default), uses the task scheduler to process messages.
        /// When true, process each message synchronously before fetching the next one.
        /// </summary>
        public bool HandleMessagesSynchronously { get; set; }

        /// <summary>
        /// Default set of settings.
        /// </summary>
        public static HareConsumerSettings GetDefaultSettings()
        {
            return new HareConsumerSettings
                {
                    ConnectionFactory = new ConnectionFactory
                        {
                            HostName = "localhost",
                            Port = 5672,
                            UserName = ConnectionFactory.DefaultUser,
                            Password = ConnectionFactory.DefaultPass,
                            VirtualHost = ConnectionFactory.DefaultVHost,
                            RequestedHeartbeat = 60,
                        },
                    MaxConnectionRetry = 5,
                    IntervalConnectionTries = TimeSpan.FromSeconds(5),
                    MaxWorkers = 1,
                    AcknowledgeMessageForMe = true,
                    TaskScheduler = TaskScheduler.Default,
                    HandleMessagesSynchronously = false,
                };
        }
    }


    public sealed class RabbitConsumer : RabbitConnectorCommon
    {
        private readonly RabbitQueue _myQueue;
        internal BaseConsumer MyConsumer;
        private string _myConsumerTag;
        private readonly object _starting = new object();

        /// <summary>
        /// Event handler for messages. If you modify this after Start methed is called, it won't be applied
        /// until next restart (=connection issue)
        ///  If you forgot to set this one, the consumer will swallow messages as fast as it can
        /// </summary>
        public event BasicDeliverEventHandler MessageHandler;

        /// <summary>
        /// Event handler for messages handler failure. If you modify this after Start method is called, it won't be applied
        /// until next restart (connection issue). If this throws an error, you are screwed, buddy. Don't tempt the devil !
        /// Handler that is called when :
        /// 1)the messageHandler throws an exception
        /// 2)the consumer itself throws an exception.
        /// You have to decide wether to ack the message in both case (even if AcknowledgeMessageForMe is set to true)
        /// </summary>
        public event ThreadedConsumer.CallbackExceptionEventHandlerWithMessage ErrorHandler;

        /// <summary>
        /// Handler called at each start (and restart). If you modify this after Start method is called, it won't be applied
        /// until next restart (connection issue). If this throws an error, you are screwed, buddy. Don't tempt the devil !
        /// </summary>
        public event ConsumerEventHandler StartHandler;

        /// <summary>
        /// Handler called at each stop. If you modify this after Start method is called, it won't be applied
        /// until next restart (connection issue). If this throws an error, you are screwed, buddy. Don't tempt the devil !
        /// </summary>
        public event ConsumerEventHandler StopHandler;

        private HareConsumerSettings _mySettings;

        /// <summary>
        /// Create a rabbitconsumer which will consume message from a PUBLIC exchange. This exchange is supposed to be direct/fanout (otherwise use the raw constructor)
        /// </summary>
        /// <param name="settings">Settings used to construct the consumer</param>
        /// <param name="exchange">the exchange you want to listen to</param>
        public RabbitConsumer(HareConsumerSettings settings, RabbitExchange exchange)
            : this(settings)
        {
            _myQueue = new RabbitQueue(GetUniqueIdentifier(exchange.Name)) { AutoDelete = true };
            RedeclareMyTopology = m =>
                {
                    exchange.Declare(m);
                    _myQueue.Declare(m);
                    m.QueueBind(_myQueue.Name, exchange.Name, "noRKrequired");
                };
        }

        /// <summary>
        /// Create a rabbitconsumer which will consume message from a PUBLIC queue without knowing who will feed it
        /// </summary>
        /// <param name="settings">Settings used to construct the consumer</param>
        /// <param name="queue">the queue you want to listen to</param>
        public RabbitConsumer(HareConsumerSettings settings, RabbitQueue queue)
            : this(settings)
        {
            _myQueue = queue;
            RedeclareMyTopology = _myQueue.Declare;
        }

        /// <summary>
        /// Raw constructor. You give the Queue and a handler to create the topology. Use at your own risk !
        /// </summary>
        /// <param name="settings">Settings used to construct the consumer</param>
        /// <param name="queue">the queue you want to listen to. It *won't* be created, you have to create it in redeclareTopology</param>
        /// <param name="redeclareTopology">Construct your complete, twisted, complex topology :) </param>
        public RabbitConsumer(HareConsumerSettings settings, RabbitQueue queue, Action<IModel> redeclareTopology)
            : this(settings)
        {
            _myQueue = queue;
            RedeclareMyTopology = redeclareTopology;
        }

        private RabbitConsumer(HareConsumerSettings settings)
            : base(settings)
        {
            _mySettings = settings;
            _myConsumerTag = GetUniqueIdentifier("tag");
        }

        internal BaseConsumer CreateConsumer(IModel model)
        {
            if (_mySettings.HandleMessagesSynchronously)
            {
                return new SyncConsumer(model, _mySettings.AcknowledgeMessageForMe);
            }
            return new ThreadedConsumer(model, (ushort) _mySettings.MaxWorkers, _mySettings.AcknowledgeMessageForMe,
                                        _mySettings.TaskScheduler ?? TaskScheduler.Default);
        }

        internal override void SpecificRestart(IModel model)
        {
            MyConsumer = CreateConsumer(model);
            if (_mySettings.CancelationTime.HasValue)
                MyConsumer.ShutdownTimeout = (int) Math.Min(_mySettings.CancelationTime.Value.TotalMilliseconds, int.MaxValue);
            MyConsumer.OnStart += StartHandler;
            MyConsumer.OnStop += StopHandler;
            MyConsumer.OnShutdown += GetShutdownHandler(); //automatically restart a new consumer in case of failure
            MyConsumer.OnDelete += GetDeleteHandler(); //automatically restart a new consumer in case of failure
            MyConsumer.OnError += ErrorHandler;

            MyConsumer.OnMessage += MessageHandler;
        }

        /// <summary>
        /// Start consuming messages. Not thread safe ! You should call it once. This api might call it at each connection failure.
        /// </summary>
        [Obsolete("Specify the maximum number of initial retries and check the returned value")]
        public void Start()
        {
            Start(_mySettings.MaxConnectionRetry, null);
        }

        /// <summary>
        /// Start consuming messages. This method is not thread safe.
        /// </summary>
        /// <param name="maxConnectionRetry">number of allowed retries before giving up</param>
        /// <returns>true if connection has succeeded</returns>
        public bool Start(int maxConnectionRetry)
        {
            return Start(maxConnectionRetry, null);
        }

        private bool Start(int maxConnectionRetry, ConnectionFailureException e)
        {
            lock (_starting)
            {
                var succeeded = InternalStart(maxConnectionRetry);
                if (succeeded)
                {
                    // The false for noHack is mandatory. Otherwise it will simply dequeue messages all the time.
                    if (_myConsumerTag != null)
                        Model.BasicConsume(_myQueue.Name, false, _myConsumerTag, MyConsumer);
                    else
                        _myConsumerTag = Model.BasicConsume(_myQueue.Name, false, MyConsumer);
                }
                return succeeded;
            }
        }

        public ConsumerShutdownEventHandler GetShutdownHandler()
        {
            //Will restart everything, that is the connection, the model, the consumer.
            //All messages that were already in treatment are lost and will be delivered again,
            //unless you have taken the responsability to ack messages
            return (_, shutdownEventArgs) =>
                {
                    var e = new ConnectionFailureException(shutdownEventArgs);
                    Start(_mySettings.MaxConnectionRetry, e);
                };
        }

        public ConsumerEventHandler GetDeleteHandler()
        {
            //Will restart everything, that is the connection, the model, the consumer.
            //All messages that were already in treatment are lost and will be delivered again,
            //unless you have taken the responsability to ack messages
            return (_, consumerEventArgs) =>
                {
                    var e = new ConnectionFailureException(consumerEventArgs);
                    Start(_mySettings.MaxConnectionRetry, e);
                };
        }

        public override void Dispose()
        {
            base.Dispose();
            if (Model != null)
            {
                if (_myConsumerTag != null)
                    Model.BasicCancel(_myConsumerTag);
                Model.Close();
            }
        }

        /// <summary>
        /// This int is used to keep track of already attributed ids.
        /// </summary>
        private static int _uid = 0;

        /// <summary>
        /// Provides a unique identifier.
        /// The result is unique on a given server+process for a given prefix
        /// It provides garantees to avoid collisions in a reasonnable setup.
        /// </summary>
        /// <param name="prefix"></param>
        /// <returns></returns>
        private static string GetUniqueIdentifier(string prefix)
        {
            var uid = Interlocked.Increment(ref _uid);
            return prefix + "-" + GetIPProcessId() + "-" + uid;
        }

        private static string GetIPProcessId()
        {
            string localIP = null;
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (!IPAddress.IsLoopback(ip) && ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    localIP = ip.ToString();
                    break;
                }
            }
            return (localIP ?? "0.0.0.0") + "-" + Process.GetCurrentProcess().Id;
        }
    }
}