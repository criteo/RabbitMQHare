using System;
using System.Diagnostics;
using System.Threading;
using Moq;
using NUnit.Framework;
using RabbitMQ.Client;

namespace RabbitMQHare.UTest
{
    class RabbitPublisher
    {
        private Mock<IModel> _model;
        private ManualResetEventSlim _mre;
        private RabbitMQHare.RabbitPublisher publisher;

        [SetUp]
        public void Setup()
        {
            var props = new Mock<IBasicProperties>();
            _model = new Mock<IModel>();
            _model.Setup(m => m.CreateBasicProperties()).Returns(props.Object);
            _mre = new ManualResetEventSlim(false);


            var connection = new Mock<IConnection>();
            connection.Setup(c => c.CreateModel()).Returns(_model.Object);

            var settings = HarePublisherSettings.GetDefaultSettings();
            settings.IntervalConnectionTries = TimeSpan.Zero;
            settings.MaxMessageWaitingToBeSent = 1;
            publisher = new RabbitMQHare.RabbitPublisher(settings, new RabbitExchange("testing"))
                {
                    CreateConnection = () => connection.Object,
                    RedeclareMyTopology = model => { },
                };
        }

        [TearDown]
        public void TearDown()
        {
            if (publisher != null)
                publisher.Dispose();
        }

        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public void BasicSend(bool blockingPublish)
        {
            _model.Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IBasicProperties>(), It.IsAny<byte[]>())).Callback(() => _mre.Set());
            publisher.Start();
            Assert.IsTrue(publisher.Started);

            var message = new byte[] {0, 1, 1};
            if (blockingPublish)
                publisher.BlockingPublish("toto", message);
            else
                publisher.Publish("toto", message);

            _mre.Wait(1000);
            _model.Verify(m => m.BasicPublish("testing", "toto", publisher._props, message));
        }

        [Test]
        public void NonEnqueued()
        {
            var called = false;
            publisher.NotEnqueuedHandler += () => called = true;
            publisher.Start();
            Assert.IsTrue(publisher.Started);

            var message = new byte[] {0, 1, 1};
            _mre = new ManualResetEventSlim(true);
            _model.Setup(m => m.BasicPublish("testing", "toto", publisher._props, message)).Callback(() => _mre.Wait(10000));

            for (var i = 0; i < 2; ++i)
                publisher.Publish("toto", message);
            _mre.Set();

            Assert.IsTrue(called);
        }
    }
}