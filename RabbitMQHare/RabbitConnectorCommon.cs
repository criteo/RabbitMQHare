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
using System.Collections.Generic;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace RabbitMQHare
{
    /// <summary>
    /// Default interface for settings used by Consumer and Publisher
    /// </summary>
    public interface IHareSettings
    {
        /// <summary>
        /// Factory used to create the connection to rabbitmq. All passwords, endpoints, ports settings go there.
        /// </summary>
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

    /// <summary>
    /// The base class that implements connection handling, reconnection and topology declaration
    /// </summary>
    public abstract class RabbitConnectorCommon : IDisposable
    {
        /// <summary>
        /// Event type used to announce a temporary connection failure (see TemporaryConnectionFailureHandler)
        /// </summary>
        /// <param name="e">Exception raised by the connection failure</param>
        public delegate void TemporaryConnectionFailure(Exception e);

        /// <summary>
        /// Event type used to announce a permanent connection failure (see PermanentConnectionFailureHandler)
        /// </summary>
        /// <param name="e">Exception raised by the connection failure</param>
        public delegate void PermanentConnectionFailure(BrokerUnreachableException e);

        /// <summary>
        /// Event type used to announce an authorization failure (see ACLFailureHandler)
        /// </summary>
        /// <param name="e"></param>
        public delegate void ACLFailure(Exception e);

        /// <summary>
        /// Called when another handler throws an exception
        /// </summary>
        /// <param name="e">Exception raised by the auth error</param>
        public delegate void EventHandlerFailure(Exception e);

        internal IConnection Connection;
        internal IModel Model;
        private readonly IHareSettings _settings;
        internal Action<IModel> RedeclareMyTopology;
        internal abstract void SpecificRestart(IModel model);
        internal Func<IConnection> CreateConnection;

        protected readonly CancellationTokenSource Cancellation;
        internal bool HasAlreadyStartedOnce = false;

        public virtual void Dispose()
        {
            Cancellation.Cancel();
        }

        /// <summary>
        /// Called when an exception is thrown when connecting to rabbit. It is called at most [MaxConnectionRetry] times before a more serious BrokerUnreachableException is thrown
        /// </summary>
        public event TemporaryConnectionFailure TemporaryConnectionFailureHandler;

        /// <summary>
        /// Called when too many exceptions ([MaxConnectionRetry]) are thrown when connecting to rabbit.
        /// The object (Consumer or Publisher) will stop trying to connect and can be considered dead when this event is called
        /// </summary>
        public event PermanentConnectionFailure PermanentConnectionFailureHandler;

        /// <summary>
        /// Called when a ACL exception is thrown.
        /// </summary>
        public event ACLFailure ACLFailureHandler;

        /// <summary>
        /// Called when an exception is thrown by another handler, this obviously must not throw an exception (it will crash)
        /// </summary>
        public event EventHandlerFailure EventHandlerFailureHandler;

        /// <summary>
        /// Use a generic event handler on all available events. Useful for debug
        /// </summary>
        /// <param name="genericHook"></param>
        internal void PlugGenericHook(Action<string, Exception> genericHook)
        {
            ACLFailureHandler += e => genericHook("ACL error", e);
            PermanentConnectionFailureHandler += e => genericHook("Permanent connection exception, won't try again ",e);
            TemporaryConnectionFailureHandler += e => genericHook("Temporary connection exception, will try again.",e);
            //we don't hook in EventHandlerFailureHandler
        }

        internal RabbitConnectorCommon(IHareSettings settings)
        {
            _settings = settings;
            CreateConnection = () => settings.ConnectionFactory.CreateConnection();
            Cancellation = new CancellationTokenSource();
        }

        internal bool InternalStart(int maxConnectionRetry, ConnectionFailureException callReason = null)
        {
            var ok = false;
            var retries = 0;
            var exceptions = new Dictionary<AmqpTcpEndpoint, Exception>(1);
            var attempts = new Dictionary<AmqpTcpEndpoint, int>(1);
            if (HasAlreadyStartedOnce)
                OnTemporaryConnectionFailureFailure(callReason);
            while (!ok && (retries++ <= maxConnectionRetry || maxConnectionRetry == -1 || maxConnectionRetry == Timeout.Infinite) && !Cancellation.IsCancellationRequested)
            {
                try
                {
                    if (Model != null)
                    {
                        try
                        {
                            Model.Close();
                        }
                        catch
                        {
                            // best effort to close the previous channel, ignore errors
                        }
                    }

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
            return ok;
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
            // otherwise can't do anything
        }


    }
}
