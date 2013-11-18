using System;

namespace RabbitMQHare
{
    /// <summary>
    /// This exception class encapsulate the various reason for a connection to be reset.
    /// The CausedByShutdown indicates which property to look for.
    /// </summary>
    public class ConnectionFailureException : Exception
    {
        public RabbitMQ.Client.Events.ConsumerEventArgs ConsumerEventArgs { get; private set; }

        public RabbitMQ.Client.ShutdownEventArgs ShutdownEventArgs { get; private set; }

        public ConnectionFailureException(RabbitMQ.Client.ShutdownEventArgs shutdownEventArgs)
            : base("Caused by a client shutdown, see ShutdownEventArgs property")
        {
            ShutdownEventArgs = shutdownEventArgs;
            CausedByShutdown = true;
        }

        public ConnectionFailureException(RabbitMQ.Client.Events.ConsumerEventArgs consumerEventArgs)
            : base("Caused by a consumer failure, see ConsumerEventArgs proprety")
        {
            ConsumerEventArgs = consumerEventArgs;
            CausedByShutdown = false;
        }

        public bool CausedByShutdown
        {
            get;
            private set;
        }
    }
}
