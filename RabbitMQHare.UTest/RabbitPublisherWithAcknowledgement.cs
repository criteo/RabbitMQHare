using System;
using System.Threading;
using Moq;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQHare.UTest
{
    class RabbitPublisherWithAcknowledgement
    {
        private Mock<IModel> _model;
        private ManualResetEventSlim _mreForFirstMessage;
        private RabbitMQHare.RabbitPublisher _publisher;
        private long _seqNumber;
        private Mock<IConnection> _connection;
        private byte[] message = new byte[] { 0, 1, 1 };

        [TearDown]
        public void TearDown()
        {
            if (_publisher != null)
                _publisher.Dispose();
        }

        [SetUp]
        public void Setup()
        {
            _seqNumber = 1;
            var props = new Mock<IBasicProperties>();
            _model = new Mock<IModel>();
            _model.Setup(m => m.CreateBasicProperties()).Returns(props.Object);
            _mreForFirstMessage = new ManualResetEventSlim(false);

            _connection = new Mock<IConnection>();
            _connection.Setup(c => c.CreateModel()).Returns(_model.Object);

            var settings = HarePublisherSettings.GetDefaultSettings();
            settings.UseConfirms = true;
            settings.IntervalConnectionTries = TimeSpan.Zero;
            settings.MaxMessageWaitingToBeSent = 1;
            _publisher = new RabbitMQHare.RabbitPublisher(settings, new RabbitExchange("testing"))
                {
                    CreateConnection = () => _connection.Object,
                    RedeclareMyTopology = model => { }
                };

            _model.Setup(m => m.NextPublishSeqNo).Returns((ulong)_seqNumber).Callback(() => Interlocked.Increment(ref _seqNumber));
        }

        [Test]
        public void ReceiveAck()
        {
            _model.Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IBasicProperties>(), It.IsAny<byte[]>())).Callback(() => _mreForFirstMessage.Set());
            _publisher.Start();

            _publisher.Publish("test", message);
            Assert.IsTrue(_mreForFirstMessage.Wait(1000));

            _model.Raise(m => m.BasicAcks += null, _model.Object, new BasicAckEventArgs() {DeliveryTag = 1, Multiple = false});

            Assert.That(_publisher._unacked.Count, Is.EqualTo(0).After(5000, 50));
        }

        [Test]
        public void ReceiveNAck()
        {
            _model.Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IBasicProperties>(), It.IsAny<byte[]>())).Callback(() => _mreForFirstMessage.Set());
            var called = false;
            _publisher.OnNAcked += m => called = true;

            _publisher.Start();

            _publisher.Publish("test", message);
            Assert.IsTrue(_mreForFirstMessage.Wait(1000));

            _model.Raise(m => m.BasicNacks += null, _model.Object, new BasicNackEventArgs() {DeliveryTag = 1, Multiple = false});

            Assert.That(_publisher._unacked.Count, Is.EqualTo(0).After(5000, 50));
            Assert.IsTrue(called);
        }

        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public void ReceiveNAckAndRequeue(bool ackMultiple)
        {
            var mreForResendMessage = new ManualResetEventSlim(false);
            var newSet = _publisher.MySettings;
            newSet.RequeueMessageAfterFailure = true;
            _publisher = new RabbitMQHare.RabbitPublisher(newSet, new RabbitExchange("testing"))
                {
                    CreateConnection = () => _connection.Object,
                    RedeclareMyTopology = model => { }
                };
            _model.Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IBasicProperties>(), It.IsAny<byte[]>())).Callback(() =>
                {
                    //on the first message we receive we set the first mre,
                    //for the second one, we set the second
                    if (_mreForFirstMessage.IsSet)
                        mreForResendMessage.Set();
                    else
                        _mreForFirstMessage.Set();
                });
            var currentSeqNumber = _seqNumber;
            var callbackCalled = new ManualResetEventSlim(false);
            _publisher.OnNAcked += _ => callbackCalled.Set();

            _publisher.Start();

            _publisher.Publish("test", message);
            Assert.IsTrue(_mreForFirstMessage.Wait(1000), "the message should be sent on the wire after a short time");

            _model.Raise(m => m.BasicNacks += null, _model.Object, new BasicNackEventArgs() {DeliveryTag = (ulong)currentSeqNumber, Multiple = ackMultiple});

            Assert.IsTrue(mreForResendMessage.Wait(1000), "the message should be resent after a short time, because the connection is still usable");
            Assert.That(_publisher._unacked.Count, Is.EqualTo(1).After(5000, 50), "when a message is on the wire, we should have an unacked message");
            Assert.IsTrue(callbackCalled.Wait(1000), "the OnNAck callback should have been called");
        }
    }
}