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
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQHare
{
    /// <summary>
    /// A class that dispatches messages synchronously.
    ///
    /// That means the OnMessage event should be connected only to things
    /// that return _really_ quickly.
    /// </summary>
    public class SyncConsumer : BaseConsumer
    {
        public SyncConsumer(IModel model, bool autoAck)
            : base(model, autoAck)
        {
            Model.BasicQos(0, 1, false);
        }

        protected override void ProcessOne(BasicDeliverEventArgs e)
        {
            try
            {
                DispatchMessage(e);
                if (AutoAck) Model.BasicAck(e.DeliveryTag, false);
            }
            catch (Exception exception)
            {
                DispatchError(e, exception);
            }
        }
    }
}
