using System;
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
        /// When connection fails, indicates the maximum numbers of tries before calling the permanent connection failure handler. DefaultSettings sets it to 5.-1 Is infinite
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

        public static readonly HareConsumerSettings DefaultSettings = new HareConsumerSettings
        {
            ConnectionFactory = new ConnectionFactory() {
                HostName = "localhost",
                Port = 5672, 
                UserName = ConnectionFactory.DefaultUser, 
                Password = ConnectionFactory.DefaultPass, 
                VirtualHost = ConnectionFactory.DefaultVHost, 
                RequestedHeartbeat = 60},
            MaxConnectionRetry = 5,
            IntervalConnectionTries = TimeSpan.FromSeconds(5),
            MaxWorkers = 1,
            AcknowledgeMessageForMe = true,
            TaskScheduler = TaskScheduler.Default
        };
    }


    public class RabbitConsumer : RabbitConnectorCommon
    {
        private readonly RabbitQueue _myQueue;
        private ThreadedConsumer _myConsumer;
        private string _myConsumerTag;

        /// <summary>
        /// Event handler for messages. If you modify this after Start methed is called, it won't be applied 
        /// until next restart (connection issue)
        ///  If you forgot to set this one, the consumer will swallow messages as fast as it can
        /// </summary>
        public event BasicDeliverEventHandler MessageHandler;

        /// <summary>
        /// Event handler for messages handler failure. If you modify this after Start methed is called, it won't be applied 
        /// until next restart (connection issue). If this throws an error, you are screwed, buddy. Don't tempt the devil !
        /// Handler that is called when 
        /// 1)the messageHandler throws an exception 
        /// 2)the consumer itself throws an exception. 
        /// You have to ack the message in both case (even if AcknowledgeMessageForMe is set to true)
        /// </summary>
        public event ThreadedConsumer.CallbackExceptionEventHandlerWithMessage ErrorHandler;

        /// <summary>
        /// Handler called at each start (and restart). If you modify this after Start methed is called, it won't be applied 
        /// until next restart (connection issue). If this throws an error, you are screwed, buddy. Don't tempt the devil !
        /// </summary>
        public event ConsumerEventHandler StartHandler;

        /// <summary>
        /// Handler called at each stop. If you modify this after Start methed is called, it won't be applied 
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
            _myQueue = new RabbitQueue(exchange.Name + "-" + random.Next()) { AutoDelete = true };
            RedeclareMyTolology = m =>
            {
                exchange.Declare(m);
                _myQueue.Declare(m);
                m.QueueBind(_myQueue.Name, exchange.Name, "toto");
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
            RedeclareMyTolology = _myQueue.Declare;
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
            RedeclareMyTolology = redeclareTopology;
        }

        private RabbitConsumer(HareConsumerSettings settings)
            : base(settings)
        {
            _mySettings = settings;
        }

        #region deprecated constructors
        /// <summary>
        /// Create a rabbitconsumer which will consume message from a PUBLIC (topic) exchange
        /// </summary>
        /// <param name="settings">Settings used to construct the consumer</param>
        /// <param name="exchangeName">the exchange you want to listen to</param>
        /// <param name="routingKeytoBind">the routing key binding. For instance data.* or message.for.me</param>
        /// <param name="temporaryConnectionFailureHandler">Handler called when there is a temporary connection failure</param>
        /// <param name="permanentConnectionFailureHandler">Handler called when there is a permanent connection failure. When called you can consider your publisher as dead</param>
        /// <param name="startHandler">Handler that will be called when the consumer is starting</param>
        /// <param name="stopHandler">Handler that will be called when the consumer stops</param>
        /// <param name="errorHandler">Handler that is called when 1)the messageHandler throws an exception 2)the consumer itself throws an exception. You have to ack the message in both case (even if AcknowledgeMessageForMe is set to true)</param>
        /// <param name="messageHandler">Handler called for each message received. If you forgot to set this one, the consumer will swallow messages as fast as it can</param>
        [Obsolete("Use constructor without optional parameters")]
        public RabbitConsumer(HareConsumerSettings settings, string exchangeName, string routingKeytoBind,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null,
            ConsumerEventHandler startHandler = null,
            ConsumerEventHandler stopHandler = null,
            ThreadedConsumer.CallbackExceptionEventHandlerWithMessage errorHandler = null,
            BasicDeliverEventHandler messageHandler = null)
            : this(settings, temporaryConnectionFailureHandler, permanentConnectionFailureHandler, startHandler, stopHandler, errorHandler, messageHandler)
        {
            var exchange = new RabbitExchange(exchangeName) {Type = ExchangeType.Topic, AutoDelete = false};
            _myQueue = new RabbitQueue(exchange.Name + "-" + random.Next()) {AutoDelete = true};
            RedeclareMyTolology = m =>
            {
                exchange.Declare(m);
                _myQueue.Declare(m);
                m.QueueBind(_myQueue.Name, exchange.Name, routingKeytoBind);
            };
        }


        /// <summary>
        /// Create a rabbitconsumer which will consume message from a PUBLIC (fanout) exchange
        /// </summary>
        /// <param name="settings">Settings used to construct the consumer</param>
        /// <param name="exchange">the exchange you want to listen to</param>
        /// <param name="temporaryConnectionFailureHandler">Handler called when there is a temporary connection failure</param>
        /// <param name="permanentConnectionFailureHandler">Handler called when there is a permanent connection failure. When called you can consider your publisher as dead</param>
        /// <param name="startHandler">Handler that will be called when the consumer is starting</param>
        /// <param name="stopHandler">Handler that will be called when the consumer stops</param>
        /// <param name="errorHandler">Handler that is called when 1)the messageHandler throws an exception 2)the consumer itself throws an exception. You have to ack the message in both case (even if AcknowledgeMessageForMe is set to true)</param>
        /// <param name="messageHandler">Handler called for each message received. If you forgot to set this one, the consumer will swallow messages as fast as it can</param>
        [Obsolete("Use constructor without optional parameters")]
        public RabbitConsumer(HareConsumerSettings settings, RabbitExchange exchange,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null,
            ConsumerEventHandler startHandler = null,
            ConsumerEventHandler stopHandler = null,
            ThreadedConsumer.CallbackExceptionEventHandlerWithMessage errorHandler = null,
            BasicDeliverEventHandler messageHandler = null)
            : this(settings, temporaryConnectionFailureHandler, permanentConnectionFailureHandler, startHandler, stopHandler, errorHandler, messageHandler)
        {
            _myQueue = new RabbitQueue(exchange.Name + "-" + random.Next()) {AutoDelete = true};
            RedeclareMyTolology = m =>
            {
                m.ExchangeDeclare(exchange.Name, ExchangeType.Fanout);
                _myQueue.Declare(m);
                m.QueueBind(_myQueue.Name, exchange.Name, "toto");
            };
        }

        /// <summary>
        /// Create a rabbitconsumer which will consume message from a PUBLIC queue without knowing who will feed it
        /// </summary>
        /// <param name="settings">Settings used to construct the consumer</param>
        /// <param name="queue">the queue you want to listen to</param>
        /// <param name="temporaryConnectionFailureHandler">Handler called when there is a temporary connection failure</param>
        /// <param name="permanentConnectionFailureHandler">Handler called when there is a permanent connection failure. When called you can consider your publisher as dead</param>
        /// <param name="startHandler">Handler that will be called when the consumer is starting</param>
        /// <param name="stopHandler">Handler that will be called when the consumer stops</param>
        /// <param name="errorHandler">Handler that is called when 1)the messageHandler throws an exception 2)the consumer itself throws an exception. You have to ack the message in both case (even if AcknowledgeMessageForMe is set to true)</param>
        /// <param name="messageHandler">Handler called for each message received. If you forgot to set this one, the consumer will swallow messages as fast as it can</param>
        [Obsolete("Use constructor without optional parameters")]
        public RabbitConsumer(HareConsumerSettings settings, RabbitQueue queue,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null,
            ConsumerEventHandler startHandler = null,
            ConsumerEventHandler stopHandler = null,
            ThreadedConsumer.CallbackExceptionEventHandlerWithMessage errorHandler = null,
            BasicDeliverEventHandler messageHandler = null)
            : this(settings, temporaryConnectionFailureHandler, permanentConnectionFailureHandler, startHandler, stopHandler, errorHandler, messageHandler)
        {
            _myQueue = queue;
            RedeclareMyTolology = m => _myQueue.Declare(m);
        }

        /// <summary>
        /// Raw constructor. You give the Queue and a handler to create the topology. Use at your own risk !
        /// </summary>
        /// <param name="settings">Settings used to construct the consumer</param>
        /// <param name="queue">the queue you want to listen to. It won't be created, you have to create it in redeclareTopology</param>
        /// <param name="redeclareTopology">Construct your complete, twisted, complex topology :) </param>
        /// <param name="temporaryConnectionFailureHandler">Handler called when there is a temporary connection failure</param>
        /// <param name="permanentConnectionFailureHandler">Handler called when there is a permanent connection failure. When called you can consider your publisher as dead</param>
        /// <param name="startHandler">Handler that will be called when the consumer is starting</param>
        /// <param name="stopHandler">Handler that will be called when the consumer stops</param>
        /// <param name="errorHandler">Handler that is called when 1)the messageHandler throws an exception 2)the consumer itself throws an exception. You have to ack the message in both case (even if AcknowledgeMessageForMe is set to true)</param>
        /// <param name="messageHandler">Handler called for each message received. If you forgot to set this one, the consumer will swallow messages as fast as it can</param>
        [Obsolete("Use constructor without optional parameters")]
        public RabbitConsumer(
            HareConsumerSettings settings, RabbitQueue queue, Action<IModel> redeclareTopology,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null,
            ConsumerEventHandler startHandler = null,
            ConsumerEventHandler stopHandler = null,
            ThreadedConsumer.CallbackExceptionEventHandlerWithMessage errorHandler = null,
            BasicDeliverEventHandler messageHandler = null)
            : this(settings,temporaryConnectionFailureHandler, permanentConnectionFailureHandler, startHandler, stopHandler, errorHandler, messageHandler)
        {
            _myQueue = queue;
            RedeclareMyTolology = redeclareTopology;
        }

        [Obsolete("Use constructor without optional parameters")]
        private RabbitConsumer(HareConsumerSettings settings,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null,
            ConsumerEventHandler startHandler =null,
            ConsumerEventHandler stopHandler = null,
            ThreadedConsumer.CallbackExceptionEventHandlerWithMessage errorHandler = null,
            BasicDeliverEventHandler messageHandler = null,
            ACLFailure aclFailureHandler =null,
            EventHandlerFailure eventFailureHandler =null)
            : base(settings)
        {
            _mySettings = settings;
            if (startHandler != null) StartHandler += startHandler;
            if (stopHandler != null) StopHandler += stopHandler;
            if (errorHandler != null) ErrorHandler += errorHandler;
            if (messageHandler != null) MessageHandler += messageHandler;

            if (temporaryConnectionFailureHandler != null) TemporaryConnectionFailureHandler += temporaryConnectionFailureHandler;
            if (permanentConnectionFailureHandler != null) PermanentConnectionFailureHandler += permanentConnectionFailureHandler;
            if (aclFailureHandler != null) ACLFailureHandler += aclFailureHandler;
            if (eventFailureHandler != null) EventHandlerFailureHandler += eventFailureHandler;

        }
        #endregion

        internal override void SpecificRestart(IModel model)
        {
            _myConsumer = new ThreadedConsumer(model, (ushort)_mySettings.MaxWorkers, _mySettings.AcknowledgeMessageForMe, _mySettings.TaskScheduler ?? TaskScheduler.Default);
            if (_mySettings.CancelationTime.HasValue)
                _myConsumer.ShutdownTimeout = (int)Math.Min(_mySettings.CancelationTime.Value.TotalMilliseconds, int.MaxValue);
            _myConsumer.OnStart += StartHandler;
            _myConsumer.OnStop += StopHandler;
            _myConsumer.OnShutdown += GetShutdownHandler(); //automatically restart a new consumer in case of failure
            _myConsumer.OnDelete += GetDeleteHandler(); //automatically restart a new consumer in case of failure
            _myConsumer.OnError += ErrorHandler;

            _myConsumer.OnMessage += MessageHandler;
        }

        /// <summary>
        /// Start consuming messages. Not thread safe ! You should call it once. This api might call it at each connection failure.
        /// </summary>
        public void Start()
        {
            InternalStart();
            // The false for noHack is mandatory. Otherwise it will simply dequeue messages all the time.
            _myConsumerTag = Model.BasicConsume(_myQueue.Name, false, _myConsumer);
        }

        public ConsumerShutdownEventHandler GetShutdownHandler()
        {
            //Will restart everything, that is the connection, the model, the consumer. 
            //All messages that were already in treatment are lost and will be delivered again, 
            //unless you have taken the responsability to ack messages
            return (_, __) => Start();
        }

        public ConsumerEventHandler GetDeleteHandler()
        {
            //Will restart everything, that is the connection, the model, the consumer. 
            //All messages that were already in treatment are lost and will be delivered again, 
            //unless you have taken the responsability to ack messages
            return (_, __) => Start();
        }

        public override void Dispose()
        {
            if (_myConsumerTag != null)
                Model.BasicCancel(_myConsumerTag);
            Model.Close();
        }
    }
}
