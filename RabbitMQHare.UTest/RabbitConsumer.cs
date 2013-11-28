using System;
using System.Threading;
using Moq;
using NUnit.Framework;
using RabbitMQ.Client;
using RabbitMQ.Client.Framing.v0_9_1;

namespace RabbitMQHare.UTest
{
    class RabbitConsumer
    {
        private RabbitMQHare.RabbitConsumer _consumer;
        private Mock<IModel> _model;

        [SetUp]
        public void Setup()
        {
            var set = HareConsumerSettings.GetDefaultSettings();
            set.HandleMessagesSynchronously = true;
            var props = new Mock<IBasicProperties>();
            _model = new Mock<IModel>();
            _model.Setup(m => m.CreateBasicProperties()).Returns(props.Object);

            var connection = new Mock<IConnection>();
            connection.Setup(c => c.CreateModel()).Returns(_model.Object);

            _consumer = new RabbitMQHare.RabbitConsumer(set, new RabbitExchange("testing"))
                {
                    CreateConnection = () => connection.Object,
                    RedeclareMyTopology = model => { },
                };
        }

        [TearDown]
        public void TearDown()
        {
            if (_consumer != null)
                _consumer.Dispose();
        }

        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public void ReceiveAMessage(bool failInHandler)
        {
            //we use integer instead of boolean to be sure handlers are called only once.
            var received = 0;
            _consumer.MessageHandler += (_, __) => { 
                ++received;
                if (failInHandler)
                    throw new Exception("An exception");
            };
            var error = 0;
            _consumer.ErrorHandler += (_, __, ___) => Interlocked.Increment(ref error);
            _consumer.Start();

            Assert.IsTrue(_consumer.HasAlreadyStartedOnce);

            _consumer._myConsumer.HandleBasicDeliver("toto", 1, false, "testing", "routingKey", new BasicProperties(), new byte[] { 0, 1, 0 });

            Assert.AreEqual(1, received);

            if (failInHandler)
                Assert.AreEqual(1, error);
        }

    }
}
