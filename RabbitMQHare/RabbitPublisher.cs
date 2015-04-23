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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using RabbitMQ.Client;
using System.Threading.Tasks;

namespace RabbitMQHare
{
    /// <summary>
    /// Settings used to construct a publisher. If you don't use HarePublisherSettings.DefaultSettings you have to fill all parameters.
    /// </summary>
    public struct HarePublisherSettings : IHareSettings
    {
        /// <summary>
        /// Maximum number of message waiting to be sent. Above this limit, new messages won't be added. DefaultSettings sets it to 10000
        /// </summary>
        public int MaxMessageWaitingToBeSent { get; set; }

        /// <summary>
        /// You can provide a way to modify the IBasicProperties of all messages. DefaultSettings sets it to  "text/plain" type and transient messages
        /// </summary>
        public IBasicProperties Properties {get; set;}

        /// <summary>
        /// RabbitMQ object to specify how to connect to rabbitMQ. DefaultSettings sets it to localhost:5672 on virtual host "/"
        /// </summary>
        public ConnectionFactory ConnectionFactory { get; set; }

        /// <summary>
        /// When connection fails, indicates the maximum numbers of retries before calling the permanent connection failure handler.
        /// DefaultSettings sets it to 5. -1 Is infinite.
        /// This setting is used only when connection has been already established once.
        /// </summary>
        public int MaxConnectionRetry { get; set; }

        /// <summary>
        /// Interval between two retries when connecting to rabbitmq. Default is 5 seconds
        /// </summary>
        public TimeSpan IntervalConnectionTries { get; set; }

        /// <summary>
        /// Prefer to send a message twice than loosing it
        /// Order is not garanteed. If you use this with UseConfirm, then message are requeued at most once.
        /// </summary>
        public bool RequeueMessageAfterFailure { get; set; }

        /// <summary>
        /// Use publisher lightweight confirms
        /// </summary>
        public bool UseConfirms { get; set; }

        /// <summary>
        /// Return a minimal set of settings to work properly.
        /// It connects to localhost using default user for rabbitmq.
        /// </summary>
        /// <returns></returns>
        public static HarePublisherSettings GetDefaultSettings()
        {
            return new HarePublisherSettings
            {
                ConnectionFactory = new ConnectionFactory { HostName = "localhost", Port = 5672, UserName = "guest", Password = "guest", VirtualHost = "/", RequestedHeartbeat = 60 },
                MaxConnectionRetry = 5,
                IntervalConnectionTries = TimeSpan.FromSeconds(5),
                MaxMessageWaitingToBeSent = 10000,
                Properties = new RabbitMQ.Client.Framing.BasicProperties
                {
                    ContentType = "text/plain",
                    DeliveryMode = 1,
                },
                UseConfirms = false,
            };
        }
    }

    /// <summary>
    /// A publisher object allow to send messages to rabbitmq.
    /// </summary>
    public sealed class RabbitPublisher : RabbitConnectorCommon, IPublisher
    {
        /// <summary>
        /// The properties sent by default with each messages
        /// </summary>
        internal IBasicProperties Properties { get; set; }

        private readonly ConcurrentQueue<Message> _internalQueue;
        private readonly AutoResetEvent _sendingMessagesMutex = new AutoResetEvent(true);

        private readonly ManualResetEventSlim _stopping = new ManualResetEventSlim();
        private long inFlightMessages = 0;
        private readonly ManualResetEventSlim _stoppable = new ManualResetEventSlim();

        private readonly RabbitExchange _myExchange;
        private readonly Thread _send;

        // signal object for message waiting in queue
        private readonly object _messagesWaiting = new object();
        private readonly object _tokenBlocking = new object();

        //We use a dictionnary instead of a queue because there is no guarantee that
        //acks or nacks come in order :
        //https://groups.google.com/forum/#!msg/rabbitmq-discuss/0O8Dick9xGA/ZF2_D8QeTzAJ
        //(the answer is from M. Radestock, technical lead of rmq)
        internal ConcurrentDictionary<ulong, Message> _unacked;

        /// <summary>
        /// Event type used when a message could not be enqueued (see NotEnqueuedHandler)
        /// </summary>
        [Obsolete("This event type has been replaced with MessageNotEnqueued")]
        public delegate void NotEnqueued();

        /// <summary>
        /// Event type used when a message could not be enqueued (see MessageNotEnqueued)
        /// </summary>
        public delegate void MessageNotEnqueued(Message message);

        /// <summary>
        /// Called when queue is full and message are not enqueued.
        /// </summary>
        [Obsolete("Use MessageNotEnqueuedHandler instead")]
        public event NotEnqueued NotEnqueuedHandler
        {
            add
            {
                MessageNotEnqueuedHandler += _ => value();
            }
            remove
            {
                MessageNotEnqueuedHandler -= _ => value();
            }
        }

        /// <summary>
        /// Called when queue is full and message are not enqueued.
        /// </summary>
        public event MessageNotEnqueued MessageNotEnqueuedHandler;

        /// <summary>
        /// Event type used when the object start and restart (connect and reconnect).
        /// See OnStart.
        /// </summary>
        /// <param name="pub"></param>
        public delegate void StartHandler(RabbitPublisher pub);

        /// <summary>
        /// Handler called at each start (and restart)
        /// This is different from StartHandler of consumer, so you can modify this at any time
        /// </summary>
        public event StartHandler OnStart;

        /// <summary>
        /// Event type used when receiving Nack messages (see OnNAcked)
        /// </summary>
        /// <param name="nackedMessages"></param>
        public delegate void NAckedHandler(ICollection<Message> nackedMessages);

        /// <summary>
        /// Handler called when some messages are not acknowledged after being published.
        /// It can happen if rabbitmq send a nacked message, or if the connection is lost
        /// before ack are received.
        /// Using this handler has no sense when UseConfirms setting is false.
        /// </summary>
        public event NAckedHandler OnNAcked;

        private void OnMessageNotEnqueuedHandler(Message message)
        {
            var copy = MessageNotEnqueuedHandler; //see http://stackoverflow.com/questions/786383/c-sharp-events-and-thread-safety
            //this behavior allow thread safety and allow to expose event publicly
            if (copy != null)
                try
                {
                    copy(message);
                }
                catch (Exception e)
                {
                    OnEventHandlerFailure(e);
                }
        }

        private void OnStartHandler()
        {
            var copy = OnStart; //see http://stackoverflow.com/questions/786383/c-sharp-events-and-thread-safety
            //this behavior allow thread safety and allow to expose event publicly
            if (copy != null)
                try
                {
                    copy(this);
                }
                catch (Exception e)
                {
                    OnEventHandlerFailure(e);
                }
        }

        private void OnNAckHandler(ICollection<Message> losts)
        {
            var copy = OnNAcked; //see http://stackoverflow.com/questions/786383/c-sharp-events-and-thread-safety
            //this behavior allow thread safety and allow to expose event publicly
            if (copy != null)
                try
                {
                    copy(losts);
                }
                catch (Exception e)
                {
                    OnEventHandlerFailure(e);
                }
        }

        /// <summary>
        /// Use a generic event handler on all available events. Useful for debug.
        /// The hook will be passed a short message and an exception (may be null)
        /// </summary>
        /// <param name="genericHook"></param>
        public void PlugGenericHook(Action<string, Exception> genericHook)
        {
            base.PlugGenericHook(genericHook);
            MessageNotEnqueuedHandler += m => genericHook("A message has not been enqueued. Message: "+ m, null);
            OnNAcked += messages => genericHook("Some messages (" + messages.Count + ") were not acked.", null);
            OnStart += self => genericHook("Publisher "+ self + " has been started", null);
        }

        /// <summary>
        /// The settings of the publisher
        /// </summary>
        public HarePublisherSettings MySettings { get; internal set; }

        /// <summary>
        /// Returns #messages waiting in the destination queue.
        /// It makes no sense to call this when sending messages to an exchange (it will throw an exception)
        /// This will also throw an exception when the connection is broken.
        /// Calling MessageCount can be useful to build some kind of flow control by polling it.
        /// </summary>
        public uint MessageCount
        {
            get
            {
                if (_destinationQueue != null)
                {
                    IConnection connection = Connection;
                    try
                    {
                        using (var tmpModel = connection.CreateModel())
                        {
                            var response = _destinationQueue.Declare(tmpModel);
                            return response.MessageCount;
                        }
                    }
                    catch
                    {
                        //next message publish operation will detect broken connection
                        //so we don't act here
                        throw new InvalidOperationException("Connection is currently broken");
                    }
                }
                throw new InvalidOperationException("This publisher target an exchange, it makes no sense to count messages in an exchange");
            }
        }

        /// <summary>
        /// This is true if the publisher has been sucessfully started once.
        /// </summary>
        public bool Started { get; private set; }

        private readonly object _starting = new object();
        private RabbitQueue _destinationQueue;

        /// <summary>
        /// Publish to a PUBLIC queue
        /// </summary>
        /// <param name="mySettings">Settings used to construct a publisher</param>
        /// <param name="destinationQueue">public queue you want to connect to</param>
        public RabbitPublisher(HarePublisherSettings mySettings, RabbitQueue destinationQueue)
            : this(mySettings)
        {
            _destinationQueue = destinationQueue;
            _myExchange = new RabbitExchange(destinationQueue.Name + "-" + "exchange") { AutoDelete = true };
            RedeclareMyTopology = m =>
            {
                _myExchange.Declare(m);
                destinationQueue.Declare(m);
                m.QueueBind(destinationQueue.Name, _myExchange.Name, "noRKneeded");
            };
        }

        /// <summary>
        /// Publish to a PUBLIC exchange
        /// </summary>
        /// <param name="mySettings">Settings used to construct a publisher</param>
        /// <param name="exchange">public exchange you want to connect to</param>
        public RabbitPublisher(HarePublisherSettings mySettings, RabbitExchange exchange)
            : this(mySettings)
        {
            _myExchange = exchange;
            RedeclareMyTopology = _myExchange.Declare;
        }

        /// <summary>
        /// Raw constructor, you have to do everything yourself
        /// </summary>
        /// <param name="mySettings"> </param>
        /// <param name="exchange">Exchange you will sent message to. It *won't* be created, you have to create it in the redeclareToplogy parameter</param>
        /// <param name="redeclareTopology">Lambda called everytime we need to recreate the rabbitmq oblects, it should be idempotent</param>
        public RabbitPublisher(HarePublisherSettings mySettings, RabbitExchange exchange, Action<IModel> redeclareTopology)
            : this(mySettings)
        {
            _myExchange = exchange;
            RedeclareMyTopology = redeclareTopology;
        }

        private RabbitPublisher(HarePublisherSettings settings)
            : base(settings)
        {
            _send = new Thread(() => DequeueSend(Cancellation.Token));
            Started = false;
            MySettings = settings;

            _internalQueue = new ConcurrentQueue<Message>();
            if (MySettings.UseConfirms && MySettings.RequeueMessageAfterFailure)
                OnNAcked += lostMessages =>
                {
                    foreach (var m in lostMessages)
                        if (m.Failed < 2)
                            Publish(m.RoutingKey, m.Payload);
                };
        }

        /// <summary>
        /// Start to publish. This method is NOT thread-safe. Advice is to use it once.
        /// </summary>
        [Obsolete("Specify the number of initial connection retries and check the returned value")]
        public void Start()
        {
            Start(MySettings.MaxConnectionRetry);
        }

        /// <summary>
        /// Start to publish messages. This method is not thread-safe.
        /// </summary>
        /// <param name="maxConnectionRetry"></param>
        /// <returns>true if the connection has succeeded. If false, you can retry to connect.</returns>
        public bool Start(int maxConnectionRetry)
        {
            lock (_starting)
            {
                var succeeded = InternalStart(maxConnectionRetry);
                if (!Started)
                {
                    // A thread name does not have to be unique. We consider that
                    // the exchange name is qualifing enough.
                    _send.Name = "RabbitPublisher:" + _myExchange.Name;
                    _send.Start();
                    Started = true;
                }
                //no more modifying after this point
                OnStartHandler();
                return succeeded;
            }
        }

        /// <summary>
        /// Method that will be called everytime there is a network failure.
        /// </summary>
        internal override void SpecificRestart(IModel model)
        {
            Properties = MySettings.Properties ?? model.CreateBasicProperties();

            // all messages on the wire are lost (or will be reenqueued by the user)
            if (HasAlreadyStartedOnce)
            {
                Interlocked.Exchange(ref inFlightMessages, _internalQueue.Count); // race condition here: messages could be published between count read and affectation
            }

            if (MySettings.UseConfirms)
            {
                model.ConfirmSelect();
                //requeue all messages that were on the wire. This might lead to duplicates.
                if (MySettings.RequeueMessageAfterFailure && _unacked != null)
                {
                    OnNAckHandler(UnAckedMessages);
                }
                _unacked = new ConcurrentDictionary<ulong, Message>();
                model.BasicAcks += (_, args) => HandleAcknowledgement(false, args.DeliveryTag, args.Multiple);
                model.BasicNacks += (_, args) => HandleAcknowledgement(true, args.DeliveryTag, args.Multiple);
            }
        }

        private List<Message> UnAckedMessages
        {
            get
            {
                var unacked = new List<Message>(_unacked.Count);
                foreach (var m in _unacked.Values)
                {
                    unacked.Add(new Message
                    {
                        Failed = m.Failed + 1,
                        RoutingKey = m.RoutingKey,
                        Payload = m.Payload
                    });
                }
                return unacked;
            }
        }

        private void HandleAcknowledgement(bool nack, ulong deliveryTag, bool multiple)
        {
            Message m;
            List<Message> lostMessages = null;
            if (multiple)
            {
                /* looping on the dictionnary while it is modified is safe.
                 * it may not reflect the last state of the dictionnary but any value that may be added
                 * after we start the loop is always above the threshold to be deleted. */
                foreach (var tag in _unacked.Keys)
                {
                    if (tag <= deliveryTag
                        && _unacked.TryRemove(tag, out m))
                    {
                        if (nack)
                        {
                            if (lostMessages == null) lostMessages = new List<Message>();
                            m.Failed += 1;
                            lostMessages.Add(m);
                        }
                        DecreaseInFlightMessages();
                    }
                }
            }
            else
            {
                if (_unacked.TryRemove(deliveryTag, out m))
                {
                    if (nack)
                    {
                        m.Failed += 1;
                        lostMessages = new List<Message> { m };
                    }
                        DecreaseInFlightMessages();
                }
            }
            if (lostMessages != null && lostMessages.Count > 0)
                OnNAckHandler(lostMessages);
        }

        /// <summary>
        /// The main thread method : dequeue and publish
        /// </summary>
        /// <returns></returns>
        private void DequeueSend(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                Message res;
                if (_internalQueue.TryPeek(out res))
                {
                    if (_sendingMessagesMutex.WaitOne(TimeSpan.Zero))
                    {
                        try
                        {
                            while (_internalQueue.TryDequeue(out res) && !token.IsCancellationRequested)
                            {
                                lock (_tokenBlocking)
                                {
                                    Monitor.Pulse(_tokenBlocking);
                                }
                                try
                                {
                                    SendMessage(res);
                                }
                                catch (RabbitMQ.Client.Exceptions.AlreadyClosedException e)
                                {
                                    HandleClosedConnection(res, e);
                                }
                                catch
                                {
                                    HandleGeneralException(res);
                                }
                            }
                        }
                        finally
                        {
                            _sendingMessagesMutex.Set();
                        }
                    }
                }
                else
                {
                    lock (_messagesWaiting)
                    {
                        Monitor.Wait(_messagesWaiting);
                    }
                }
            }
        }

        private void HandleGeneralException(Message res)
        {
            HandleFailedMessage(res, MySettings.RequeueMessageAfterFailure);

            //No need to offer any event handler since reconnection will probably fail at the first time and the standard handlers will be called
            Start(MySettings.MaxConnectionRetry);
        }

        private void HandleClosedConnection(Message res, RabbitMQ.Client.Exceptions.AlreadyClosedException e)
        {
            HandleFailedMessage(res, MySettings.RequeueMessageAfterFailure);

            switch (e.ShutdownReason.ReplyCode)
            {
                case RabbitMQ.Client.Framing.Constants.AccessRefused:
                    OnACLFailure(e);
                    break;
                default:
                    Start(MySettings.MaxConnectionRetry);
                    break;
            }
        }

        private void HandleFailedMessage(Message res, bool retry)
        {
            if (retry)
            {
                //don't need to increase inflight messages
                _internalQueue.Enqueue(res);
            }
            else
            {
                DecreaseInFlightMessages();
            }
        }

        private void SendMessage(Message res)
        {
            var next = Model.NextPublishSeqNo;
            var props = res.Properties ?? Properties;
            Model.BasicPublish(_myExchange.Name, res.RoutingKey, props, res.Payload);
            if (MySettings.UseConfirms)
            {
                _unacked.TryAdd(next, res);
            }
            else
            {
                // if we don't use confirms, a sent message in considered to be ok
                DecreaseInFlightMessages();
            }
        }

        /// <summary>
        /// Add message that will be sent asynchronously. This method is thread-safe
        /// </summary>
        /// <param name="routingKey">routing key used to route the message. If not needed just put "toto"</param>
        /// <param name="message">the message you want to send</param>
        /// <returns>false if the message was droppped instead of added to the queue</returns>
        public bool Publish(string routingKey, byte[] message)
        {
            return Publish(routingKey, message, null); //optional parameter? no, see CS0854
        }

        /// <summary>
        /// Add message that will be sent asynchronously. This method is thread-safe
        /// </summary>
        /// <param name="routingKey">routing key used to route the message. If not needed just put "toto"</param>
        /// <param name="message">the message you want to send</param>
        /// <param name="messageProperties">properties of the message. If set, overrides ConstructProperties parameter</param>
        /// <returns>false if the message was droppped instead of added to the queue</returns>
        public bool Publish(string routingKey, byte[] message, IBasicProperties messageProperties)
        {
            var _message = new Message { RoutingKey = routingKey, Payload = message, Properties = messageProperties };
            if (_internalQueue.Count >= MySettings.MaxMessageWaitingToBeSent)
            {
                OnMessageNotEnqueuedHandler(_message);
                return false;
            }
            Enqueue(_message);
            lock (_messagesWaiting)
            {
                Monitor.Pulse(_messagesWaiting);
            }
            return true;
        }
        ///<summary>
        /// Add message that will be sent asynchronously BUT might block if queue is full. This method is thread-safe
        /// </summary>
        /// <param name="routingKey">routing key used to route the message. If not needed just put "toto"</param>
        /// <param name="message">the message you want to send</param>
        /// <param name="messageProperties">properties of the message. If set, overrides ConstructProperties parameter</param>
        public void BlockingPublish(string routingKey, byte[] message)
        {
            BlockingPublish(routingKey, message, null);
        }
        /// <summary>
        /// Add message that will be sent asynchronously BUT might block if queue is full. This method is thread-safe
        /// </summary>
        /// <param name="routingKey">routing key used to route the message. If not needed just put "toto"</param>
        /// <param name="message">the message you want to send</param>
        /// <param name="messageProperties">properties of the message. If set, overrides ConstructProperties parameter</param>
        public void BlockingPublish(string routingKey, byte[] message, IBasicProperties messageProperties)
        {
            if (_internalQueue.Count >= MySettings.MaxMessageWaitingToBeSent)
            {
                lock (_tokenBlocking)
                {
                    // Check the queue length again to ensure that we don't enter the Wait if the queue was emptied before we got the lock
                    if (_internalQueue.Count >= MySettings.MaxMessageWaitingToBeSent)
                    {
                        //TODO : instead of being unblocked by each dequeue, add a low watermark
                        Monitor.Wait(_tokenBlocking);
                    }
                }
            }
            var _message = new Message { RoutingKey = routingKey, Payload = message, Properties = messageProperties };
            Enqueue(_message);
            lock (_messagesWaiting)
            {
                Monitor.Pulse(_messagesWaiting);
            }
        }

        /// <summary>
        /// Check for stopping state, increment messages in flight count and enqueue.
        /// This method will throw an exception if messages are sent
        /// </summary>
        /// <param name="message"></param>
        private void Enqueue(Message message)
        {
            if (_stopping.IsSet)
            {
                throw new ClientStoppingException("Publisher is stopping, you cannot publish new messages");
            }
            Interlocked.Increment(ref inFlightMessages);
            _internalQueue.Enqueue(message);
        }

        private void DecreaseInFlightMessages()
        {
            if (Interlocked.Decrement(ref inFlightMessages) <= 0 && _stopping.IsSet)
            {
                _stoppable.Set();
            }
        }

        /// <summary>
        /// Trigger stop process. As soon as this method is called, message enqueing may trigger exceptions.
        /// It is the user responsability not to call any Publish method (directly, through an event handler, ...).
        /// When the returned mutex is set, all messages have been sent.
        /// </summary>
        /// <returns>A mutex to wait on for stop process completion.</returns>
        public ManualResetEventSlim Stop()
        {
            _stopping.Set();
            if (Interlocked.Read(ref inFlightMessages) <= 0)
            {
                _stoppable.Set();
            }
            return _stoppable;
        }

        /// <summary>
        /// Blocking method to wait for stop process completion. At the end, this publisher is disposable.
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns>true if stop process completed successfully, false otherwise. In the latter case, messages have probably been lost</returns>
        public bool GracefulStop(TimeSpan timeout)
        {
            var stopped = Stop();
            return stopped.Wait(timeout);
        }

        public override void Dispose()
        {
            base.Dispose();
            lock (_messagesWaiting)
            {
                Monitor.Pulse(_messagesWaiting);      // unlock DequeueSend thread for exit
            }

            _sendingMessagesMutex.WaitOne(); // wait to exit the dequeue/send loop

            if (Model != null)
                Model.Dispose();
            _stoppable.Dispose();
            _stopping.Dispose();
        }
    }

    /// <summary>
    /// Structure representing an in-flight message.
    /// This is used to account for Acked and NAcked messages.
    /// </summary>
    public struct Message
    {
        /// <summary>
        /// Routing key used when sending the message
        /// </summary>
        public string RoutingKey { get; set; }

        /// <summary>
        /// Binary payload of the message
        /// </summary>
        public byte[] Payload { get; set; }

        /// <summary>
        /// Number of times this message has been send and have failed.
        /// </summary>
        public int Failed;

        public IBasicProperties Properties { get; set; }
    }
}
