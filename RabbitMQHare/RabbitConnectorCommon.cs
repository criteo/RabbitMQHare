﻿using System;
using System.Collections.Generic;
using System.Threading;
using RabbitMQ.Client;

namespace RabbitMQHare
{
    public interface IHareSettings
    {
        ConnectionFactory ConnectionFactory { get; set; }
        int MaxConnectionRetry { get; set; }

        /// <summary>
        /// Time between two retries when connecting to rabbitmq. Default is 5 seconds
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
        public System.Collections.IDictionary Arguments { get; set; }
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
        public System.Collections.IDictionary Arguments { get; set; }
    }

    public abstract class RabbitConnectorCommon :  IDisposable
    {
        public delegate void TemporaryConnectionFailure(Exception e);
        public delegate void PermanentConnectionFailure(RabbitMQ.Client.Exceptions.BrokerUnreachableException e);

        internal IConnection Connection;
        internal IModel Model;
        private readonly IHareSettings _settings;

        internal Action<IModel> RedeclareMyTolology;

        internal abstract void SpecificRestart(IModel model);
        public abstract void Dispose();

        [ThreadStatic]
        internal static Random r = null;
        internal static Random random { get { return r ?? (r = new Random()); }
        }

        /// <summary>
        /// Called when an exception is thrown when connecting to rabbit. It is called at most [MaxConnectionRetry] times before a more serious BrokerUnreachableException is thrown
        /// </summary>
        private event TemporaryConnectionFailure TemporaryConnectionFailureHandler;

        /// <summary>
        /// Called when too many exceptions ([MaxConnectionRetry]) are thrown when connecting to rabbit.
        /// </summary>
        private event PermanentConnectionFailure PermanentConnectionFailureHandler;

        internal RabbitConnectorCommon(
            IHareSettings settings,
            TemporaryConnectionFailure temporaryConnectionFailureHandler,
            PermanentConnectionFailure permanentConnectionFailureHandler
            )
        {
            _settings = settings;
            if (temporaryConnectionFailureHandler != null) TemporaryConnectionFailureHandler += temporaryConnectionFailureHandler;
            if (permanentConnectionFailureHandler != null) PermanentConnectionFailureHandler += permanentConnectionFailureHandler;
            
        }

        internal void InternalStart()
        {
            bool ok = false;
            int retries = 0;
            var exceptions = new Dictionary<int, Exception>(_settings.MaxConnectionRetry);
            var attempts = new Dictionary<int, string>(_settings.MaxConnectionRetry);
            while (!ok && ++retries < _settings.MaxConnectionRetry)
            {
                try
                {
                    Connection = _settings.ConnectionFactory.CreateConnection();
                    Model = Connection.CreateModel();
                    Connection.AutoClose = true;
                    RedeclareMyTolology(Model);
                    SpecificRestart(Model);
                    ok = true;
                }
                catch (Exception e)
                {
                    exceptions[retries] = e;
                    attempts[retries] = string.Format("{1} : Attempt {0}", retries, DateTime.UtcNow);
                    if (TemporaryConnectionFailureHandler != null) TemporaryConnectionFailureHandler(e);
                    Thread.Sleep(_settings.IntervalConnectionTries);
                }
            }
            if (!ok)
            {
                var e = new RabbitMQ.Client.Exceptions.BrokerUnreachableException(attempts, exceptions);
                if (PermanentConnectionFailureHandler != null) this.PermanentConnectionFailureHandler(e);
            }
        }

        
    }
}
