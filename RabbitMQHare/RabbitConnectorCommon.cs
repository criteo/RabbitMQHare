using System;
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
        /// Maximum numbers of unsuccessful connection retry. 
        /// -1 (System.Threading.Timeout.Infinite) is infinite
        /// </summary>
        int MaxConnectionRetry { get; set; }

        /// <summary>
        /// Interval between two retries when connecting to rabbitmq. Default is 5 seconds
        /// </summary>
        TimeSpan IntervalConnectionTries { get; set; }
    }

    public abstract class RabbitConnectorCommon : IDisposable
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
        internal Action<IModel> RedeclareMyTopology;
        internal abstract void SpecificRestart(IModel model);
        internal Func<IConnection> CreateConnection;

        protected readonly CancellationTokenSource _cancellation;
        internal bool HasAlreadyStartedOnce = false;

        public virtual void Dispose()
        {
            _cancellation.Cancel();
        }

        protected Random _random = new Random();

        /// <summary>
        /// Called when an exception is thrown when connecting to rabbit. It is called at most [MaxConnectionRetry] times before a more serious BrokerUnreachableException is thrown
        /// </summary>
        public event TemporaryConnectionFailure TemporaryConnectionFailureHandler;

        /// <summary>
        /// Called when too many exceptions ([MaxConnectionRetry]) are thrown when connecting to rabbit.
        /// </summary>
        public event PermanentConnectionFailure PermanentConnectionFailureHandler;

        /// <summary>
        /// Called when a ACL exception is thrown
        /// </summary>
        public event ACLFailure ACLFailureHandler;

        /// <summary>
        /// Called when an exception is thrown by another handler, this obviously should not throw exception (it will crash)
        /// </summary>
        public event EventHandlerFailure EventHandlerFailureHandler;


        internal RabbitConnectorCommon(IHareSettings settings)
        {
            _settings = settings;
            CreateConnection = () => settings.ConnectionFactory.CreateConnection();
            _cancellation = new CancellationTokenSource();
        }

        internal void InternalStart(ConnectionFailureException callReason = null)
        {
            var ok = false;
            var retries = 0;
            var exceptions = new Dictionary<AmqpTcpEndpoint, Exception>(1);
            var attempts = new Dictionary<AmqpTcpEndpoint, int>(1);
            if (HasAlreadyStartedOnce)
                OnTemporaryConnectionFailureFailure(callReason);
            while (!ok && (++retries < _settings.MaxConnectionRetry || _settings.MaxConnectionRetry == -1 || _settings.MaxConnectionRetry == Timeout.Infinite) && !_cancellation.IsCancellationRequested)
            {
                try
                {
                    Connection = CreateConnection();
                    Model = Connection.CreateModel();
                    Connection.AutoClose = true;
                    TryRedeclareTopology();
                    SpecificRestart(Model);
                    ok = true;
                    HasAlreadyStartedOnce = true;
                }
                catch (Exception e)
                {
                    var endpoint = new AmqpTcpEndpoint();
                    if (_settings.ConnectionFactory != null && _settings.ConnectionFactory.Endpoint != null)
                        endpoint = _settings.ConnectionFactory.Endpoint;
                    exceptions[endpoint] = e;
                    attempts[endpoint] = retries;
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
                RedeclareMyTopology(Model);
            }
            catch (OperationInterruptedException e)
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
