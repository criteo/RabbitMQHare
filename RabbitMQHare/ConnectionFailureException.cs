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
﻿using System;

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
            : base("Caused by a consumer failure, see ConsumerEventArgs property")
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
