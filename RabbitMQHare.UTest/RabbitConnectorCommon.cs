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
using Moq;
using NUnit.Framework;
using RabbitMQ.Client;

namespace RabbitMQHare.UTest
{
    internal class RabbitConnectorCommon
    {
        private TestContext CreateContext()
        {
            var model = new Mock<IModel>();
            var conf = new Mock<IHareSettings>();
            conf.Setup(a => a.MaxConnectionRetry).Returns(5);
            return new TestContext()
                {
                    Conf = conf,
                    Model = model
                };
        }

        [Test]
        public void BasicTest()
        {
            using (var testContext = CreateContext())
            {
                var connection = new Mock<IConnection>();
                connection.Setup(c => c.CreateModel()).Returns(testContext.Model.Object);
                testContext.Stub = new StubConnectorCommon(testContext.Conf.Object)
                    {
                        //silently replace the connection getter
                        CreateConnection = () => connection.Object,
                        RedeclareMyTopology = _ => { },
                    };

                Assert.IsTrue(testContext.Stub.InternalStart(0), "connection should succeed");

                Assert.IsTrue(testContext.Stub.HasAlreadyStartedOnce);
            }
        }

        [Test]
        public void RelunctantConnectivity()
        {
            using (var testContext = CreateContext())
            {
                var connection = new Mock<IConnection>();
                var calls = 0;
                const int maxCalls = 3;
                connection.Setup(c => c.CreateModel()).Returns(testContext.Model.Object).Callback(() => { if (++calls < maxCalls) throw new Exception("Argh I fail to connect !"); });

                testContext.Stub = new StubConnectorCommon(testContext.Conf.Object)
                    {
                        //silently replace the connection getter
                        CreateConnection = () => connection.Object,
                        RedeclareMyTopology = _ => { },
                    };

                Assert.True(testContext.Stub.InternalStart(maxCalls), "Connection should succeed after a few tentatives");

                Assert.IsTrue(testContext.Stub.HasAlreadyStartedOnce);
            }
        }

        internal class TestContext : IDisposable
        {
            public StubConnectorCommon Stub { get; set; }
            public Mock<IModel> Model { get; set; }
            public Mock<IHareSettings> Conf { get; set; }

            public void Dispose()
            {
                if (Stub != null)
                    Stub.Dispose();
            }
        }
    }

    internal class StubConnectorCommon : RabbitMQHare.RabbitConnectorCommon
    {
        public override void Dispose()
        {
        }

        internal override void SpecificRestart(RabbitMQ.Client.IModel model)
        {
        }

        public StubConnectorCommon(IHareSettings set) : base(set)
        {
        }
    }
}