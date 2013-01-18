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
        /// When connection fails, indicates the maximum numbers of tries before calling the permanent connection failure handler. DefaultSettings sets it to 5. -1 Is infinite
        /// </summary>
        public int MaxConnectionRetry { get; set; }

        /// <summary>
        /// Interval between two retries when connecting to rabbitmq. Default is 5 seconds
        /// </summary>
        public TimeSpan IntervalConnectionTries { get; set; }

        public static readonly HarePublisherSettings DefaultSettings = new HarePublisherSettings
        {
            ConnectionFactory = new ConnectionFactory() { HostName = "localhost", Port = 5672, UserName = "guest", Password = "guest", VirtualHost = "/", RequestedHeartbeat = 60 },
            MaxConnectionRetry = 5,
            IntervalConnectionTries = TimeSpan.FromSeconds(5),
            MaxMessageWaitingToBeSent = 10000,
            ConstructProperties = props =>
                {
                    props.ContentType = "text/plain";
                    props.DeliveryMode = 1;
                },
        };
    }

    /// <summary>
    /// TODO doc 
    /// </summary>
    public class RabbitPublisher : RabbitConnectorCommon, IDisposable
    {
        private IBasicProperties _props;
        private readonly ConcurrentQueue<KeyValuePair<string, byte[]>> _internalQueue;
        private readonly object _lock = new object();

        private readonly RabbitExchange _myExchange;
        private Task send;
        private CancellationTokenSource cancellation;
        private readonly object _token = new object();
        private readonly object _tokenBlocking = new object();

        public delegate void NotEnqueued();
        /// <summary>
        /// Called when queue is full and message are not enqueued
        /// </summary>
        public event NotEnqueued NotEnqueuedHandler;

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

        public HarePublisherSettings MySettings { get; private set; }

        public bool Started { get; private set; }

        /// <summary>
        /// Publish to a PUBLIC queue
        /// </summary>
        /// <param name="mySettings">Settings used to construct a publisher</param>
        /// <param name="destinationQueue">public queue you want to connect to</param>
        public RabbitPublisher(HarePublisherSettings mySettings, RabbitQueue destinationQueue)
            : this(mySettings)
        {
            _myExchange = new RabbitExchange(destinationQueue.Name + "-" + "exchange") { AutoDelete = true };
            RedeclareMyTolology = m =>
            {
                _myExchange.Declare(m);
                destinationQueue.Declare(m);
                m.QueueBind(destinationQueue.Name, _myExchange.Name, "toto");
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
            RedeclareMyTolology = _myExchange.Declare;
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
            RedeclareMyTolology = redeclareTopology;
        }

        private RabbitPublisher(HarePublisherSettings settings)
            : base(settings)
        {
            cancellation = new CancellationTokenSource();
            send = new Task(() => DequeueSend(cancellation.Token));
            Started = false;
            MySettings = settings;

            _internalQueue = new ConcurrentQueue<KeyValuePair<string, byte[]>>();
        }

        #region deprecated constructors
        /// <summary>
        /// Publish to a PUBLIC queue
        /// </summary>
        /// <param name="mySettings">Settings used to construct a publisher</param>
        /// <param name="destinationQueue">public queue you want to connect to</param>
        /// <param name="temporaryConnectionFailureHandler">Handler called when there is a temporary connection failure</param>
        /// <param name="permanentConnectionFailureHandler">Handler called when there is a permanent connection failure. When called you can consider your publisher as dead</param>
        /// <param name="notEnqueuedHandler">Handler called when messages are not enqueued because queue is full </param>
        [Obsolete("Use version without optional parameter")]
        public RabbitPublisher(HarePublisherSettings mySettings, RabbitQueue destinationQueue,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null,
            NotEnqueued notEnqueuedHandler =null)
            : this(mySettings, temporaryConnectionFailureHandler, permanentConnectionFailureHandler, notEnqueuedHandler)
        {
            _myExchange = new RabbitExchange(destinationQueue.Name + "-" + "exchange") { AutoDelete = true };
            RedeclareMyTolology = m =>
            {
                m.ExchangeDeclare(_myExchange.Name, _myExchange.Type, _myExchange.Durable, _myExchange.AutoDelete, _myExchange.Arguments);
                m.QueueDeclare(destinationQueue.Name, destinationQueue.Durable, destinationQueue.Exclusive, destinationQueue.AutoDelete, destinationQueue.Arguments);
                m.QueueBind(destinationQueue.Name, _myExchange.Name, "toto");
            };
        }

        /// <summary>
        /// Publish to a PUBLIC exchange
        /// </summary>
        /// <param name="mySettings">Settings used to construct a publisher</param>
        /// <param name="exchange">public exchange you want to connect to</param>
        /// <param name="temporaryConnectionFailureHandler">Handler called when there is a temporary connection failure</param>
        /// <param name="permanentConnectionFailureHandler">Handler called when there is a permanent connection failure. When called you can consider your publisher as dead</param>
        /// <param name="notEnqueuedHandler">Handler called when messages are not enqueued because queue is full  </param>
        [Obsolete("Use version without optional parameter")]
        public RabbitPublisher(HarePublisherSettings mySettings, RabbitExchange exchange,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null,
            NotEnqueued notEnqueuedHandler = null)
            : this(mySettings, temporaryConnectionFailureHandler, permanentConnectionFailureHandler, notEnqueuedHandler)
        {
            _myExchange = exchange;
            RedeclareMyTolology = m => m.ExchangeDeclare(_myExchange.Name, _myExchange.Type, _myExchange.Durable, _myExchange.AutoDelete, _myExchange.Arguments);
        }

        /// <summary>
        /// Raw constructor
        /// </summary>
        /// <param name="mySettings"> </param>
        /// <param name="exchange">Exchange you will sent message to. It won't be created, you have to create it in the redeclareToplogy parameter</param>
        /// <param name="redeclareTopology">Just create the topology you need</param>
        /// <param name="temporaryConnectionFailureHandler">Handler called when there is a temporary connection failure</param>
        /// <param name="permanentConnectionFailureHandler">Handler called when there is a permanent connection failure. When called you can consider your publisher as dead</param>
        /// <param name="notEnqueuedHandler"> Handler called when messages are not enqueued because queue is full </param>
        [Obsolete("Use version without optional parameter")]
        public RabbitPublisher(HarePublisherSettings mySettings, RabbitExchange exchange, Action<IModel> redeclareTopology,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null,
            NotEnqueued notEnqueuedHandler = null)
            : this(mySettings, temporaryConnectionFailureHandler, permanentConnectionFailureHandler, notEnqueuedHandler)
        {
            _myExchange = exchange;
            RedeclareMyTolology = redeclareTopology;
        }

        [Obsolete("Use version without optional parameter")]
        private RabbitPublisher(HarePublisherSettings settings,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null,
            NotEnqueued notEnqueuedHandler = null,
            ACLFailure aclFailureHandler = null,
            EventHandlerFailure eventFailureHandler = null)
            : this(settings)
        {
            if (notEnqueuedHandler != null) NotEnqueuedHandler += notEnqueuedHandler;

            if (temporaryConnectionFailureHandler != null) TemporaryConnectionFailureHandler += temporaryConnectionFailureHandler;
            if (permanentConnectionFailureHandler != null) PermanentConnectionFailureHandler += permanentConnectionFailureHandler;
            if (aclFailureHandler != null) ACLFailureHandler += aclFailureHandler;
            if (eventFailureHandler != null) EventHandlerFailureHandler += eventFailureHandler;

            cancellation = new CancellationTokenSource();
            send = new Task(() => DequeueSend(cancellation.Token));
            Started = false;
            MySettings = settings;

            _internalQueue = new ConcurrentQueue<KeyValuePair<string, byte[]>>();
        }
        #endregion

        /// <summary>
        /// Start to publish. This method is NOT thread-safe. Advice is to use it once.
        /// </summary>
        public void Start()
        {
            InternalStart();

            if (!Started)
            {
                send.Start();
                Started = true;
            }
            //no more modifying after this point
        }

        /// <summary>
        /// Method that will be called everytime there is a network failure.
        /// </summary>
        internal override void SpecificRestart(IModel model)
        {
            _props = model.CreateBasicProperties();
            MySettings.ConstructProperties(_props);
        }

        /// <summary>
        /// The main thread method : dequeue and publish
        /// </summary>
        /// <returns></returns>
        private void DequeueSend(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                KeyValuePair<string, byte[]> res;
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
                            string routingKey = res.Key;
                            byte[] message = res.Value;
                            try
                            {
                                Model.BasicPublish(_myExchange.Name, routingKey, _props, message);
                            }
                            catch (RabbitMQ.Client.Exceptions.AlreadyClosedException e)
                            {
                                switch (e.ShutdownReason.ReplyCode)
                                {
                                    case RabbitMQ.Client.Framing.v0_9_1.Constants.AccessRefused:
                                        OnACLFailure(e);
                                        break;
                                    default:
                                        Start();
                                        break;

                                }
                            }
                            catch
                            {
                                //No need to offer any event handler since reconnection will probably fail at the first time and the standard handlers will be called
                                Start();
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

        /// <summary>
        /// Add message that will be sent asynchronously. This method is thread-safe
        /// </summary>
        /// <param name="routingKey">routing key used to route the message. If not needed just put "toto"</param>
        /// <param name="message">the message you want to send</param>
        /// <returns>false if the message was droppped instead of added to the queue</returns>
        public bool Publish(string routingKey, byte[] message)
        {
            if (_internalQueue.Count > MySettings.MaxMessageWaitingToBeSent)
            {
                OnNotEnqueuedHandler();
                return false;
            }
            _internalQueue.Enqueue(new KeyValuePair<string, byte[]>(routingKey, message));
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
            if (_internalQueue.Count > MySettings.MaxMessageWaitingToBeSent)
            {
                lock (_tokenBlocking)
                {
                    //TODO : instead of being unblocked by each dequeue, add a low watermark
                    Monitor.Wait(_tokenBlocking);
                }
            }
            _internalQueue.Enqueue(new KeyValuePair<string, byte[]>(routingKey, message));
            lock (_token)
            {
                Monitor.Pulse(_token);
            }
        }

        public override void Dispose()
        {
            cancellation.Cancel();
            Monitor.TryEnter(_lock, TimeSpan.FromSeconds(30));
            Model.Dispose();
        }


    }
}
