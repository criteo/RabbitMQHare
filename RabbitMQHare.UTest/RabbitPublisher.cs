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
using System.Threading;
using Moq;
using NUnit.Framework;
using RabbitMQ.Client;

namespace RabbitMQHare.UTest
{
    internal class RabbitPublisher
    {
        internal class TestContext : IDisposable
        {
            public Mock<IModel> Model { get; set; }
            public ManualResetEventSlim Mre { get; set; }
            public RabbitMQHare.RabbitPublisher Publisher { get; set; }
            public Mock<IConnection> Connection { get; set; }

            public void Dispose()
            {
                if (Publisher != null)
                    Publisher.Dispose();
            }
        }

        private TestContext CreateContext(int maxMessageInTheQueue = 1)
        {
            var props = new Mock<IBasicProperties>();
            var model = new Mock<IModel>();
            model.Setup(m => m.CreateBasicProperties()).Returns(props.Object);
            var mre = new ManualResetEventSlim(false);


            var connection = new Mock<IConnection>();
            connection.Setup(c => c.CreateModel()).Returns(model.Object);

            var settings = HarePublisherSettings.GetDefaultSettings();
            settings.MaxConnectionRetry = -1;
            settings.IntervalConnectionTries = TimeSpan.Zero;
            settings.MaxMessageWaitingToBeSent = maxMessageInTheQueue;
            var publisher = new RabbitMQHare.RabbitPublisher(settings, new RabbitExchange("testing"))
                {
                    CreateConnection = () => connection.Object,
                    RedeclareMyTopology = m => { },
                };
            return new TestContext
                {
                    Connection = connection,
                    Model = model,
                    Mre = mre,
                    Publisher = publisher
                };
        }

        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public void BasicSend(bool blockingPublish)
        {
            using (var context = CreateContext())
            {
                context.Model.Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IBasicProperties>(), It.IsAny<byte[]>())).Callback(() => context.Mre.Set());
                context.Publisher.Start(0);
                Assert.IsTrue(context.Publisher.Started);

                var message = new byte[] { 0, 1, 1 };
                if (blockingPublish)
                    context.Publisher.BlockingPublish("toto", message);
                else
                    context.Publisher.Publish("toto", message);

                Assert.IsTrue(context.Mre.Wait(1000));
                context.Model.Verify(m => m.BasicPublish("testing", "toto", context.Publisher.Props, message));
            }
        }

        [Test]
        public void NonEnqueued()
        {
            using (var context = CreateContext(0))
            {
                var called = false;
                context.Publisher.NotEnqueuedHandler += () => called = true;
                context.Publisher.Start(0);
                Assert.IsTrue(context.Publisher.Started);

                var message = new byte[] { 0, 1, 1 };
                context.Mre = new ManualResetEventSlim(false);
                context.Model.Setup(m => m.BasicPublish("testing", "toto", context.Publisher.Props, message));

                Assert.False(context.Publisher.Publish("toto", message), "the last message should not to be published");

                Assert.IsTrue(called, "when too many messages are waiting to be sent, the correct callback is called");
            }
        }

        [Test]
        [Timeout(10000)]
        public void ExtremeDisposal()
        {
            using (var context = CreateContext())
            {
                Assert.AreEqual(-1, context.Publisher.MySettings.MaxConnectionRetry, "For this test, we want the worst situation");

                context.Publisher.Start(0);
                Assert.IsTrue(context.Publisher.Started);

                var message = new byte[] { 0, 1, 1 };
                var connectionFail = new SemaphoreSlim(0);
                context.Model.Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), context.Publisher.Props, message)).Throws(new Exception("I don't want your message anymore"));
                context.Connection.Setup(c => c.CreateModel()).Callback(() => connectionFail.Release(1)).Throws(new Exception("And I don't want to accept your connection either"));

                context.Publisher.Publish("test", message);

                /* The way callbacks are implemented on exception throwing mocks does not garantee
                 * that the callback is called "after" the exception is thrown.
                 * If we wait for two, we are sure at least one has been finished !
                 */
                Assert.IsTrue(connectionFail.Wait(1000));
                Assert.IsTrue(connectionFail.Wait(1000));

                context.Publisher.Dispose();
                context.Publisher = null; //to avoid the double dispose of Publisher
                //The real test here is that eventually the Dispose method returns
                Assert.Pass();
            }
        }
    }
}