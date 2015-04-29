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
using System.Collections;
using RabbitMQ.Client;

namespace RabbitMQHare
{
    public class RabbitExchange
    {
        //TODO : this class can be modified after giving it to a publisher/consumer,
        //we should have made this a struct instead, or at least use a copy operator

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
        /// Declare the exchange against a model
        /// </summary>
        /// <param name="model"></param>
        public void Declare(IModel model)
        {
            model.ExchangeDeclare(Name, Type, Durable, AutoDelete, Arguments);
        }
    }
}
