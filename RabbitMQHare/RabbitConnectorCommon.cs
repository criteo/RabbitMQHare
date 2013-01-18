using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace RabbitMQHare
{
    public interface IHareSettings
    {
        ConnectionFactory ConnectionFactory { get; set; }

        /// <summary>
        /// Maximum numbers of unsuccessful connection retry. -1 is infinite
        /// </summary>
        int MaxConnectionRetry { get; set; }

        /// <summary>
        /// Interval between two retries when connecting to rabbitmq. Default is 5 seconds
        /// </summary>
        TimeSpan IntervalConnectionTries { get; set; }
    }

    public class RabbitQueue
    {
        /// <summary>
        /// A non durable, non exclusive, non auto-delete queue.
        /// </summary>
        /// <param name="name"></param>
        public RabbitQueue(string name)
        {
            Name = name;
            Durable = false;
            Exclusive = false;
            AutoDelete = false;
        }

        public string Name { get; private set; }
        public bool Durable { get; set; }
        public bool Exclusive { get; set; }
        public bool AutoDelete { get; set; }
        public IDictionary Arguments { get; set; }

        /// <summary>
        /// Declare the queue
        /// </summary>
        /// <param name="model"></param>
        public void Declare(IModel model)
        {
            model.QueueDeclare(Name, Durable, Exclusive, AutoDelete, Arguments);
        }
    }

    public class RabbitExchange
    {
        public RabbitExchange(string name)
        {
            Name = name;
            Type = ExchangeType.Fanout;
            Durable = false;
            AutoDelete = true;
        }

        public string Name { get; private set; }
        public string Type { get; set; }
        public bool Durable { get; set; }
        public bool AutoDelete { get; set; }
        public IDictionary Arguments { get; set; }

        /// <summary>
        /// Declare the exchange
        /// </summary>
        /// <param name="model"></param>
        public void Declare(IModel model)
        {
            model.ExchangeDeclare(Name, Type, Durable, AutoDelete, Arguments);
        }
    }

    /// <summary>
    /// This class should have been set to private, in order to keep a stable api, it won't be done immediatly, so don't use it :)
    /// </summary>
    public abstract class RabbitConnectorCommon :  IDisposable
    {
        public delegate void TemporaryConnectionFailure(Exception e);
        public delegate void PermanentConnectionFailure(BrokerUnreachableException e);
        public delegate void ACLFailure(Exception e);
        /// <summary>
        /// Called when another handler throw an exception
        /// </summary>
        /// <param name="e"></param>
        public delegate void EventHandlerFailure(Exception e);

        internal IConnection Connection;
        internal IModel Model;
        private readonly IHareSettings _settings;

        internal Action<IModel> RedeclareMyTolology;

        internal abstract void SpecificRestart(IModel model);
        public abstract void Dispose();

        [ThreadStatic]
        internal static Random r = null;
        internal static Random random { get { return r ?? (r = new Random()); } }

        /// <summary>
        /// Called when an exception is thrown when connecting to rabbit. It is called at most [MaxConnectionRetry] times before a more serious BrokerUnreachableException is thrown
        /// </summary>
        public event TemporaryConnectionFailure TemporaryConnectionFailureHandler;

        /// <summary>
        /// Called when too many exceptions ([MaxConnectionRetry]) are thrown when connecting to rabbit.
        /// </summary>
        public event PermanentConnectionFailure PermanentConnectionFailureHandler;

        /// <summary>
        /// Called when a ACL is thrown
        /// </summary>
        public event ACLFailure ACLFailureHandler;

        /// <summary>
        /// Called when an exception is thrown by another handler, this obviously 
        /// </summary>
        public event EventHandlerFailure EventHandlerFailureHandler;


        internal RabbitConnectorCommon(IHareSettings settings)
        {
            _settings = settings;
        }

        internal void InternalStart()
        {
            bool ok = false;
            int retries = 0;
            var exceptions = new Dictionary<int, Exception>(Math.Max(1, _settings.MaxConnectionRetry));
            var attempts = new Dictionary<int, string>(Math.Max(1, _settings.MaxConnectionRetry));
            while (!ok && (++retries < _settings.MaxConnectionRetry ||_settings.MaxConnectionRetry == -1))
            {
                try
                {
                    Connection = _settings.ConnectionFactory.CreateConnection();
                    Model = Connection.CreateModel();
                    Connection.AutoClose = true;
                    TryRedeclareTopology();
                    SpecificRestart(Model);
                    ok = true;
                }
                catch (Exception e)
                {
                    exceptions[retries] = e;
                    attempts[retries] = string.Format("{1} : Attempt {0}", retries, DateTime.UtcNow);
                    OnTemporaryConnectionFailureFailure(e);
                    Thread.Sleep(_settings.IntervalConnectionTries);
                }
            }
            if (!ok)
            {
                var e = new BrokerUnreachableException(attempts, exceptions);
                OnPermanentConnectionFailureFailure(e);
            }
        }

        private void TryRedeclareTopology()
        {
            try
            {
                RedeclareMyTolology(Model);
            }
            catch (RabbitMQ.Client.Exceptions.OperationInterruptedException e)
            {
                if (e.ShutdownReason.ReplyCode.Equals(RabbitMQ.Client.Framing.v0_9_1.Constants.AccessRefused))
                    OnACLFailure(e);
                throw;
            }
        }


        protected void OnPermanentConnectionFailureFailure(BrokerUnreachableException e)
        {
            var copy = PermanentConnectionFailureHandler; //see http://stackoverflow.com/questions/786383/c-sharp-events-and-thread-safety
            //this behavior allow thread safety and allow to expose event publicly
            if (copy != null)
                try
                {
                    copy(e);
                }
                catch (Exception ee)
                {
                    OnEventHandlerFailure(ee);
                }
        }

        protected void OnTemporaryConnectionFailureFailure(Exception e)
        {
            var copy = TemporaryConnectionFailureHandler; //see http://stackoverflow.com/questions/786383/c-sharp-events-and-thread-safety
            //this behavior allow thread safety and allow to expose event publicly
            if (copy != null)
                try
                {
                    copy(e);
                }
                catch (Exception ee)
                {
                    OnEventHandlerFailure(ee);
                }
        }

        protected void OnACLFailure(Exception e)
        {
            var copy = ACLFailureHandler; //see http://stackoverflow.com/questions/786383/c-sharp-events-and-thread-safety
            //this behavior allow thread safety and allow to expose event publicly
            if (copy != null)
                try
                {
                    copy(e);
                }
                catch (Exception ee)
                {
                    OnEventHandlerFailure(ee);
                }
        }

        protected void OnEventHandlerFailure(Exception e)
        {
            var copy = EventHandlerFailureHandler; //see http://stackoverflow.com/questions/786383/c-sharp-events-and-thread-safety
            //this behavior allow thread safety and allow to expose event publicly
            if (copy != null)
                copy(e);
            else throw e;
        }

        
    }
}
