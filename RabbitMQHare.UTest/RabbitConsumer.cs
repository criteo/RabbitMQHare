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
using System.Threading;
using Moq;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing.v0_9_1;

namespace RabbitMQHare.UTest
{
    internal class RabbitConsumer
    {
        private TestContext CreateContext()
        {
            var set = HareConsumerSettings.GetDefaultSettings();
            set.IntervalConnectionTries = TimeSpan.Zero;
            set.HandleMessagesSynchronously = true;
            var props = new Mock<IBasicProperties>();
            var model = new Mock<IModel>(MockBehavior.Strict);
            model.Setup(m => m.CreateBasicProperties()).Returns(props.Object);
            model.Setup(m => m.BasicQos(It.IsAny<uint>(), It.IsAny<ushort>(), It.IsAny<bool>()));
            model.Setup(m => m.BasicConsume(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(), It.IsAny<BaseConsumer>())).Returns("test");
            model.Setup(m => m.BasicCancel(It.IsAny<string>()));
            model.Setup(m => m.Close());

            var connection = new Mock<IConnection>();
            connection.Setup(c => c.CreateModel()).Returns(model.Object);

            var consumer = new RabbitMQHare.RabbitConsumer(set, new RabbitExchange("testing"))
            {
                CreateConnection = () => connection.Object,
                RedeclareMyTopology = m => { },
            };
            return new TestContext()
            {
                Consumer = consumer,
                Model = model
            };
        }

        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public void ReceiveAMessage(bool failInHandler)
        {
            using (var context = CreateContext())
            {
                //we use integer instead of boolean to be sure handlers are called only once.
                var received = 0;
                context.Consumer.MessageHandler += (_, __) =>
                    {
                        ++received;
                        if (failInHandler)
                            throw new Exception("An exception");
                    };
                var error = 0;
                context.Consumer.ErrorHandler += (_, __, ___) => ++error;
                context.Consumer.Start(0);

                Assert.IsTrue(context.Consumer.HasAlreadyStartedOnce);

                context.Consumer.MyConsumer.HandleBasicDeliver("toto", 1, false, "testing", "routingKey", new BasicProperties(), new byte[] { 0, 1, 0 });

                Assert.AreEqual(1, received);

                if (failInHandler)
                    Assert.AreEqual(1, error);
            }
        }

        [Test]
        public void InitialConnectionIssues()
        {
            using (var context = CreateContext())
            {
                var permFailureHandlerCalled = new ManualResetEventSlim();
                context.Consumer.PermanentConnectionFailureHandler += e => { permFailureHandlerCalled.Set(); throw new Exception("I want the handler exception handler called!"); };
                var handlerExceptionHandlerCalled = new ManualResetEventSlim();
                context.Consumer.EventHandlerFailureHandler += e => handlerExceptionHandlerCalled.Set();
                const int maxCalls = 3;
                var calls = 0;
                context.Model.Setup(m => m.BasicQos(It.IsAny<uint>(), It.IsAny<ushort>(), It.IsAny<bool>()))
                    .Callback<uint, ushort, bool>((_, __, ___) => { if (Interlocked.Increment(ref calls) < maxCalls) throw new Exception("no connection for now"); });
                Assert.IsFalse(context.Consumer.Start(1), "We should not raise an exception and return a nice false");

                Assert.IsTrue(permFailureHandlerCalled.IsSet, "this handler should have been called before Start method return");
                Assert.IsTrue(handlerExceptionHandlerCalled.IsSet);

                Assert.IsTrue(context.Consumer.Start(0), "we should be able to start even after failure");
            }
        }

        [Test]
        public void LoosingConnection()
        {
            using (var context = CreateContext())
            {
                var received = 0;
                context.Consumer.MessageHandler += (_, __) => ++received;
                context.Model.Setup(m => m.BasicConsume(It.IsAny<string>(), It.IsAny<bool>(), It.IsAny<string>(), It.IsAny<BaseConsumer>()))
                        .Callback<string, bool, string, IBasicConsumer>((_, __, tag, consumer) => consumer.HandleBasicConsumeOk(tag))
                        .Returns("test");

                Assert.True(context.Consumer.Start(0), "connection should succeed");

                Assert.IsTrue(context.Consumer.HasAlreadyStartedOnce);

                var restarted = 0;
                context.Consumer.StartHandler += (_, e) => ++restarted;

                context.Consumer.MyConsumer.HandleModelShutdown(context.Model.Object, new ShutdownEventArgs(ShutdownInitiator.Peer, 0, "Thanks for playing"));

                Assert.AreEqual(1, restarted);
                context.Consumer.MyConsumer.HandleBasicDeliver("toto", 1, false, "testing", "routingKey", new BasicProperties(), new byte[] { 0, 1, 0 });
                Assert.AreEqual(1, received);
            }
        }

        internal class TestContext : IDisposable
        {
            public Mock<IModel> Model { get; set; }
            public RabbitMQHare.RabbitConsumer Consumer { get; set; }

            public void Dispose()
            {
                if (Consumer != null)
                    Consumer.Dispose();
            }
        }
    }
}