using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using RabbitMQ.Client;
using System.Threading.Tasks;

namespace RabbitMQHare
{
    public struct HarePublisherSettings : IHareSettings
    {
        public int MaxMessageWaitingToBeSent { get; set; }

        /// <summary>
        /// You can provide a way to modify the IBasicProperties of all the next messages. Default is "text/plain" type and transient messages
        /// </summary>
        public Action<IBasicProperties> ConstructProperties { get; set; }

        public ConnectionFactory ConnectionFactory { get; set; }
        public int MaxConnectionRetry { get; set; }

        /// <summary>
        /// Time between two retries when connecting to rabbitmq. Default is 5 seconds
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


    public class RabbitPublisher : RabbitConnectorCommon, IDisposable
    {
        private IBasicProperties _props;
        private readonly ConcurrentQueue<KeyValuePair<string, byte[]>> internalQueue;
        private readonly object _lock = new object();

        private readonly RabbitExchange _myExchange;
        private Task send;
        private CancellationTokenSource cancellation;
        private readonly object _token = new object();
        public HarePublisherSettings MySettings {get;private set;}

        public bool Started { get; private set; }

        /// <summary>
        /// Publish to a PUBLIC queue
        /// </summary>
        /// <param name="destinationQueue"></param>
        public RabbitPublisher(HarePublisherSettings mySettings, RabbitQueue destinationQueue,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null)
            : this(mySettings, temporaryConnectionFailureHandler, permanentConnectionFailureHandler)
        {
            _myExchange = new RabbitExchange(destinationQueue.Name + "-" + "exchange") {AutoDelete = true};
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
        /// <param name="pool"></param>
        /// <param name="exchange"></param>
        public RabbitPublisher(HarePublisherSettings mySettings, RabbitExchange exchange,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null)
            : this(mySettings, temporaryConnectionFailureHandler, permanentConnectionFailureHandler)
        {
            _myExchange = exchange;
            RedeclareMyTolology = m => m.ExchangeDeclare(_myExchange.Name, _myExchange.Type, _myExchange.Durable, _myExchange.AutoDelete, _myExchange.Arguments);
        }

        /// <summary>
        /// Raw constructor
        /// </summary>
        /// <param name="exchange">Exchange you will sent message to. It won't be created, you have to create it in the redeclareToplogy parameter</param>
        /// <param name="redeclareTopology">Just create the topology you need</param>
        public RabbitPublisher(HarePublisherSettings mySettings, RabbitExchange exchange, Action<IModel> redeclareTopology,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null)
            : this(mySettings, temporaryConnectionFailureHandler, permanentConnectionFailureHandler)
        {
            _myExchange = exchange;
            RedeclareMyTolology = redeclareTopology;
        }

        private RabbitPublisher(HarePublisherSettings settings,
            TemporaryConnectionFailure temporaryConnectionFailureHandler = null,
            PermanentConnectionFailure permanentConnectionFailureHandler = null)
            : base(settings, temporaryConnectionFailureHandler, permanentConnectionFailureHandler)
        {
            cancellation = new CancellationTokenSource();
            send = new Task(() => DequeueSend(cancellation.Token));
            Started = false;
            MySettings = settings;

            internalQueue = new ConcurrentQueue<KeyValuePair<string, byte[]>>();
        }

        /// <summary>
        /// Start to publish. This method is NOT thread-safe
        /// </summary>
        public void Start()
        {
            InternalStart();

            if (!Started)
            {
                send.Start();
                Started = true;
            }
            //no more modying after this point
        }

        /// <summary>
        /// Method that should be called everytime there is a network failure.
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
                if (internalQueue.TryPeek(out res))
                {
                    lock (_lock)
                    {
                        do
                        {
                            string routingKey = res.Key;
                            byte[] message = res.Value;
                            try
                            {
                                Model.BasicPublish(_myExchange.Name, routingKey, _props, message);
                                internalQueue.TryDequeue(out res); //confirm that message was correctly dequeued
                            }
                            catch
                            {
                                //No need to offer any event handler since reconnection will probably fail at the first time and the standard handlers will be called
                                Start();
                            }
                        } while (internalQueue.TryPeek(out res));
                    }
                }
                else
                {
                    //Thread.Sleep(1000);
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
        /// <param name="routingKey"></param>
        /// <param name="message"></param>
        /// <returns>false if the message was droppped instead of added to the queue</returns>
        public bool Publish(string routingKey, byte[] message)
        {
            if (internalQueue.Count > MySettings.MaxMessageWaitingToBeSent)
            {
                return false;
            }
            internalQueue.Enqueue(new KeyValuePair<string, byte[]>(routingKey, message));
            lock (_token)
            {
                Monitor.Pulse(_token);
            }
            return true;
        }

        public override void Dispose()
        {
            cancellation.Cancel();
            Monitor.TryEnter(_lock, TimeSpan.FromSeconds(30));
            Model.Dispose();
        }


    }
}
