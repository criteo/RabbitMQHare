using System;
using System.Threading;
using Moq;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQHare.UTest
{
    internal class RabbitPublisherWithAcknowledgement
    {
        public byte[] Message = new byte[] {0, 1, 1};

        internal class TestContext : IDisposable
        {
            public Mock<IModel> Model;
            public ManualResetEventSlim MreForFirstMessage;
            public RabbitMQHare.RabbitPublisher Publisher;
            public long SeqNumber;
            public Mock<IConnection> Connection;

            public void Dispose()
            {
                if (Publisher != null)
                    Publisher.Dispose();
            }
        }

        private TestContext CreateContext()
        {
            var seqNumber = 1;
            var props = new Mock<IBasicProperties>();
            var model = new Mock<IModel>();
            model.Setup(m => m.CreateBasicProperties()).Returns(props.Object);
            var mreForFirstMessage = new ManualResetEventSlim(false);

            var connection = new Mock<IConnection>();
            connection.Setup(c => c.CreateModel()).Returns(model.Object);

            var settings = HarePublisherSettings.GetDefaultSettings();
            settings.UseConfirms = true;
            settings.IntervalConnectionTries = TimeSpan.Zero;
            settings.MaxMessageWaitingToBeSent = 1;
            var publisher = new RabbitMQHare.RabbitPublisher(settings, new RabbitExchange("testing"))
            {
                CreateConnection = () => connection.Object,
                RedeclareMyTopology = m => { }
            };

            model.Setup(m => m.NextPublishSeqNo).Returns((ulong)seqNumber).Callback(() => Interlocked.Increment(ref seqNumber));
            return new TestContext
            {
                Connection = connection,
                Model = model,
                MreForFirstMessage = mreForFirstMessage,
                Publisher = publisher,
                SeqNumber = seqNumber
            };
        }

        [Test]
        public void ReceiveAck()
        {
            using (var context = CreateContext())
            {
                context.Model.Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IBasicProperties>(), It.IsAny<byte[]>())).Callback(() => context.MreForFirstMessage.Set());
                context.Publisher.Start(0);

                context.Publisher.Publish("test", Message);
                Assert.IsTrue(context.MreForFirstMessage.Wait(1000));

                context.Model.Raise(m => m.BasicAcks += null, context.Model.Object, new BasicAckEventArgs { DeliveryTag = 1, Multiple = false });

                Assert.That(context.Publisher._unacked.Count, Is.EqualTo(0).After(5000, 50));
            }
        }

        [Test]
        public void ReceiveNAck()
        {
            using (var context = CreateContext())
            {
                context.Model.Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IBasicProperties>(), It.IsAny<byte[]>())).Callback(() => context.MreForFirstMessage.Set());
                var called = false;
                context.Publisher.OnNAcked += m => called = true;

                context.Publisher.Start(0);

                context.Publisher.Publish("test", Message);
                Assert.IsTrue(context.MreForFirstMessage.Wait(1000));

                context.Model.Raise(m => m.BasicNacks += null, context.Model.Object, new BasicNackEventArgs { DeliveryTag = 1, Multiple = false });

                Assert.That(context.Publisher._unacked.Count, Is.EqualTo(0).After(5000, 50));
                Assert.IsTrue(called);
            }
        }

        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public void ReceiveNAckAndRequeue(bool ackMultiple)
        {
            using (var context = CreateContext())
            {
                var mreForResendMessage = new ManualResetEventSlim(false);
                var newSet = context.Publisher.MySettings;
                newSet.RequeueMessageAfterFailure = true;
                context.Publisher = new RabbitMQHare.RabbitPublisher(newSet, new RabbitExchange("testing"))
                    {
                        CreateConnection = () => context.Connection.Object,
                        RedeclareMyTopology = model => { }
                    };
                context.Model.Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IBasicProperties>(), It.IsAny<byte[]>())).Callback(() =>
                    {
                        //on the first Message we receive we set the first mre,
                        //for the second one, we set the second
                        if (context.MreForFirstMessage.IsSet)
                            mreForResendMessage.Set();
                        else
                            context.MreForFirstMessage.Set();
                    });
                var currentSeqNumber = context.SeqNumber;
                var callbackCalled = new ManualResetEventSlim(false);
                context.Publisher.OnNAcked += _ => callbackCalled.Set();

                context.Publisher.Start(0);

                context.Publisher.Publish("test", Message);
                Assert.IsTrue(context.MreForFirstMessage.Wait(1000), "the Message should be sent on the wire after a short time");

                context.Model.Raise(m => m.BasicNacks += null, context.Model.Object, new BasicNackEventArgs { DeliveryTag = (ulong)currentSeqNumber, Multiple = ackMultiple });

                Assert.IsTrue(mreForResendMessage.Wait(1000), "the Message should be resent after a short time, because the connection is still usable");
                Assert.That(context.Publisher._unacked.Count, Is.EqualTo(1).After(5000, 50), "when a Message is on the wire, we should have an unacked Message");
                Assert.IsTrue(callbackCalled.Wait(1000), "the OnNAck callback should have been called");
            }
        }
    }
}