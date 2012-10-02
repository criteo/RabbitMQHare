using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQHare
{
    public struct HareConsumerSettings : IHareSettings
    {
        public int MaxWorkers { get; set; }

        /// <summary>
        /// If false, it is your responsability to ack each message in your MessageHandler. Default is true
        /// </summary>
        public bool AcknowledgeMessageForMe { get; set; }

        public ConnectionFactory ConnectionFactory { get; set; }
        public int MaxConnectionRetry { get; set; }

        /// <summary>
        /// Time between two retries when connecting to rabbitmq. Default is 5 seconds
        /// </summary>
        public TimeSpan IntervalConnectionTries { get; set; }

        public static readonly HareConsumerSettings DefaultSettings = new HareConsumerSettings
        {
            ConnectionFactory = new ConnectionFactory() {HostName = "localhost",Port = 5672, UserName = "guest", Password = "guest", VirtualHost = "/", RequestedHeartbeat = 60},
            MaxConnectionRetry = 5,
            IntervalConnectionTries = TimeSpan.FromSeconds(5),
            MaxWorkers = 1,
            AcknowledgeMessageForMe = true,
        };
    }


    public class RabbitConsumer : RabbitConnectorCommon
    {
        private readonly RabbitQueue myQueue;
        private ThreadedConsumer _myConsumer;
        private string _myConsumerTag;

        private event BasicDeliverEventHandler MessageHandler;
        private event ThreadedConsumer.CallbackExceptionEventHandlerWithMessage ErrorHandler;
        private event ConsumerEventHandler StartHandler;
        private event ConsumerEventHandler StopHandler;
        private HareConsumerSettings _mySettings;

        /// <summary>
        /// Create a rabbitconsumer which will consume message from a PUBLIC (topic) exchange
        /// </summary>
        /// <param name="exchangeName"></param>
        /// <param name="routingKeytoBind"></param>
        public RabbitConsumer(HareConsumerSettings settings, string exchangeName, string routingKeytoBind,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null,
            ConsumerEventHandler startHandler = null,
            ConsumerEventHandler stopHandler = null,
            ThreadedConsumer.CallbackExceptionEventHandlerWithMessage errorHandler = null,
            BasicDeliverEventHandler messageHandler = null)
            : this(settings, temporaryConnectionFailureHandler, permanentConnectionFailureHandler, startHandler, stopHandler, errorHandler, messageHandler)
        {
            var exchange = new RabbitExchange(exchangeName);
            exchange.Type = ExchangeType.Topic;
            exchange.AutoDelete = false;
            myQueue = new RabbitQueue(exchange.Name + "-" + random.Next());
            myQueue.AutoDelete = true;
            RedeclareMyTolology = m =>
            {
                m.ExchangeDeclare(exchange.Name, exchange.Type);
                m.QueueDeclare(myQueue.Name, myQueue.Durable, myQueue.Exclusive, myQueue.AutoDelete, myQueue.Arguments);
                m.QueueBind(myQueue.Name, exchange.Name, routingKeytoBind);
            };
        }

        /// <summary>
        /// Create a rabbitconsumer which will consume message from a PUBLIC (fanout) exchange
        /// </summary>
        /// <param name="exchange"></param>
        public RabbitConsumer(HareConsumerSettings settings, RabbitExchange exchange,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null,
            ConsumerEventHandler startHandler = null,
            ConsumerEventHandler stopHandler = null,
            ThreadedConsumer.CallbackExceptionEventHandlerWithMessage errorHandler = null,
            BasicDeliverEventHandler messageHandler = null)
            : this(settings, temporaryConnectionFailureHandler, permanentConnectionFailureHandler, startHandler, stopHandler, errorHandler, messageHandler)
        {
            myQueue = new RabbitQueue(exchange.Name + "-" + random.Next()) {AutoDelete = true};
            RedeclareMyTolology = m =>
            {
                m.ExchangeDeclare(exchange.Name, ExchangeType.Fanout);
                m.QueueDeclare(myQueue.Name, myQueue.Durable, myQueue.Exclusive, myQueue.AutoDelete, myQueue.Arguments);
                m.QueueBind(myQueue.Name, exchange.Name, "toto");
            };
        }


        /// <summary>
        /// Create a rabbitconsumer which will consume message from a PUBLIC queue without knowing who will feed it
        /// </summary>
        /// <param name="queue"></param>
        public RabbitConsumer(HareConsumerSettings settings, RabbitQueue queue,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null,
            ConsumerEventHandler startHandler = null,
            ConsumerEventHandler stopHandler = null,
            ThreadedConsumer.CallbackExceptionEventHandlerWithMessage errorHandler = null,
            BasicDeliverEventHandler messageHandler = null)
            : this(settings, temporaryConnectionFailureHandler, permanentConnectionFailureHandler, startHandler, stopHandler, errorHandler, messageHandler)
        {
            myQueue = queue;
            RedeclareMyTolology = m => m.QueueDeclare(myQueue.Name, myQueue.Durable, myQueue.Exclusive, myQueue.AutoDelete, myQueue.Arguments);
        }

        /// <summary>
        /// Raw constructor. You give the Queue and a handler to create the topology. Use at your own risk !
        /// </summary>
        /// <param name="queue"></param>
        /// <param name="redeclareTopology">this action should declare all your topology, including the queue on which it </param>
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
            myQueue = queue;
            this.RedeclareMyTolology = redeclareTopology;
        }

        private RabbitConsumer(HareConsumerSettings settings,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null,
            ConsumerEventHandler startHandler =null,
            ConsumerEventHandler stopHandler = null,
            ThreadedConsumer.CallbackExceptionEventHandlerWithMessage errorHandler = null,
            BasicDeliverEventHandler messageHandler = null)
            : base(settings,temporaryConnectionFailureHandler,permanentConnectionFailureHandler)
        {
            _mySettings = settings;
            if (startHandler != null) StartHandler += startHandler;
            if (stopHandler != null) StopHandler += stopHandler;
            if (errorHandler != null) ErrorHandler += errorHandler;
            if (messageHandler != null) MessageHandler += messageHandler;
        }

        internal override void SpecificRestart(IModel model)
        {
            _myConsumer = new ThreadedConsumer(model, (ushort)_mySettings.MaxWorkers, _mySettings.AcknowledgeMessageForMe);
            _myConsumer.OnStart += StartHandler;
            _myConsumer.OnStop += StopHandler;
            _myConsumer.OnShutdown += GetShutdownHandler(); //automatically restart a new consumer in case of failure
            _myConsumer.OnDelete += GetDeleteHandler(); //automatically restart a new consumer in case of failure
            _myConsumer.OnError += ErrorHandler;

            _myConsumer.OnMessage += MessageHandler;
        }

        /// <summary>
        /// Start the consuming of messages. This method is automatically called everytime there is a network failure.
        /// </summary>
        public void Start()
        {
            InternalStart();
            // The false for noHack is mandatory. Otherwise it will simply dequeue messages all the time.
            _myConsumerTag = Model.BasicConsume(myQueue.Name, false, _myConsumer);
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
