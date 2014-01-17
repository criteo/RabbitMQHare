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
        public Action<IBasicProperties> ConstructProperties { get; set; }

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
                ConstructProperties = props =>
                {
                    props.ContentType = "text/plain";
                    props.DeliveryMode = 1;
                },
                UseConfirms = false,
            };
        }
    }

    /// <summary>
    /// A publisher object allow to send messages to rabbitmq.
    /// </summary>
    public sealed class RabbitPublisher : RabbitConnectorCommon
    {
        internal IBasicProperties Props;
        private readonly ConcurrentQueue<Message> _internalQueue;
        private readonly object _lock = new object();

        private readonly RabbitExchange _myExchange;
        private readonly Thread _send;
        private readonly object _token = new object();
        private readonly object _tokenBlocking = new object();

        //We use a dictionnary instead of a queue because there is no guarantee that
        //acks or nacks come in order :
        //https://groups.google.com/forum/#!msg/rabbitmq-discuss/0O8Dick9xGA/ZF2_D8QeTzAJ
        //(the answer is from M. Radestock, technical lead of rmq)
        internal ConcurrentDictionary<ulong, Message> _unacked;

        /// <summary>
        /// Event type used when a message could not be enqueued (see NotEnqueuedHandler)
        /// </summary>
        public delegate void NotEnqueued();

        /// <summary>
        /// Called when queue is full and message are not enqueued.
        /// </summary>
        public event NotEnqueued NotEnqueuedHandler;

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

        private void OnNotEnqueuedHandler()
        {
            var copy = NotEnqueuedHandler; //see http://stackoverflow.com/questions/786383/c-sharp-events-and-thread-safety
            //this behavior allow thread safety and allow to expose event publicly
            if (copy != null)
                try
                {
                    copy();
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
        /// The settings of the publisher
        /// </summary>
        public HarePublisherSettings MySettings { get; internal set; }

        /// <summary>
        /// This is true if the publisher has been sucessfully started once.
        /// </summary>
        public bool Started { get; private set; }

        private readonly object _starting = new object();

        /// <summary>
        /// Publish to a PUBLIC queue
        /// </summary>
        /// <param name="mySettings">Settings used to construct a publisher</param>
        /// <param name="destinationQueue">public queue you want to connect to</param>
        public RabbitPublisher(HarePublisherSettings mySettings, RabbitQueue destinationQueue)
            : this(mySettings)
        {
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
            Props = model.CreateBasicProperties();
            MySettings.ConstructProperties(Props);

            if (MySettings.UseConfirms)
            {
                model.ConfirmSelect();
                //requeue all messages that were on the wire. This might lead to duplicates.
                if (MySettings.RequeueMessageAfterFailure && _unacked != null)
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
                    OnNAckHandler(unacked);
                }
                _unacked = new ConcurrentDictionary<ulong, Message>();
                model.BasicAcks += (_, args) => HandleAcknowledgement(false, args.DeliveryTag, args.Multiple);
                model.BasicNacks += (_, args) => HandleAcknowledgement(true, args.DeliveryTag, args.Multiple);
            }
        }

        private void HandleAcknowledgement(bool nack, ulong deliveryTag, bool multiple)
        {
            Message m;
            List<Message> lostMessages = null;
            if (multiple)
            {
                /* modifying the dictionnary while it is modified is safe.
                 * it may not reflect the last state of the dictionnary but any value that may be added
                 * after we start the loop is always above the threshold to be deleted. */
                foreach (var tag in _unacked.Keys)
                {
                    if (tag <= deliveryTag
                        && _unacked.TryRemove(tag, out m)
                        && nack)
                    {
                        if (lostMessages == null) lostMessages = new List<Message>();
                        m.Failed += 1;
                        lostMessages.Add(m);
                    }
                }
                if (lostMessages != null && lostMessages.Count > 0)
                    OnNAckHandler(lostMessages);
            }
            else
            {
                if (_unacked.TryRemove(deliveryTag, out m) && nack)
                {
                    m.Failed += 1;
                    lostMessages = new List<Message> { m };
                    OnNAckHandler(lostMessages);
                }
            }
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
                    lock (_lock)
                    {
                        while (_internalQueue.TryDequeue(out res))
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
                }
                else
                {
                    lock (_token)
                    {
                        Monitor.Wait(_token);
                    }
                }
            }
        }

        private void HandleGeneralException(Message res)
        {
            if (MySettings.RequeueMessageAfterFailure)
                _internalQueue.Enqueue(res);
            //No need to offer any event handler since reconnection will probably fail at the first time and the standard handlers will be called
            Start(MySettings.MaxConnectionRetry);
        }

        private void HandleClosedConnection(Message res, RabbitMQ.Client.Exceptions.AlreadyClosedException e)
        {
            if (MySettings.RequeueMessageAfterFailure)
                _internalQueue.Enqueue(res);
            switch (e.ShutdownReason.ReplyCode)
            {
                case RabbitMQ.Client.Framing.v0_9_1.Constants.AccessRefused:
                    OnACLFailure(e);
                    break;
                default:
                    Start(MySettings.MaxConnectionRetry);
                    break;
            }
        }

        private void SendMessage(Message res)
        {
            var next = Model.NextPublishSeqNo;
            Model.BasicPublish(_myExchange.Name, res.RoutingKey, Props, res.Payload);
            if (MySettings.UseConfirms)
                _unacked.TryAdd(next, res);
        }

        /// <summary>
        /// Add message that will be sent asynchronously. This method is thread-safe
        /// </summary>
        /// <param name="routingKey">routing key used to route the message. If not needed just put "toto"</param>
        /// <param name="message">the message you want to send</param>
        /// <returns>false if the message was droppped instead of added to the queue</returns>
        public bool Publish(string routingKey, byte[] message)
        {
            if (_internalQueue.Count >= MySettings.MaxMessageWaitingToBeSent)
            {
                OnNotEnqueuedHandler();
                return false;
            }
            _internalQueue.Enqueue(new Message { RoutingKey = routingKey, Payload = message });
            lock (_token)
            {
                Monitor.Pulse(_token);
            }
            return true;
        }

        /// <summary>
        /// Add message that will be sent asynchronously BUT might block if queue is full. This method is thread-safe
        /// </summary>
        /// <param name="routingKey">routing key used to route the message. If not needed just put "toto"</param>
        /// <param name="message">the message you want to send</param>
        public void BlockingPublish(string routingKey, byte[] message)
        {
            if (_internalQueue.Count >= MySettings.MaxMessageWaitingToBeSent)
            {
                lock (_tokenBlocking)
                {
                    //TODO : instead of being unblocked by each dequeue, add a low watermark
                    Monitor.Wait(_tokenBlocking);
                }
            }
            _internalQueue.Enqueue(new Message { RoutingKey = routingKey, Payload = message });
            lock (_token)
            {
                Monitor.Pulse(_token);
            }
        }

        public override void Dispose()
        {
            base.Dispose();
            lock (_token)
            {
                Monitor.Pulse(_token);      // unlock DequeueSend thread for exit
            }

            lock (_lock) { };                    // wait to process all messages existing in internal queue

            if (Model != null)
                Model.Dispose();
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
    }
}
