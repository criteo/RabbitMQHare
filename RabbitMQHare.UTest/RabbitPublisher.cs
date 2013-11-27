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

        [SetUp]
        public void Setup()
        {
            var props = new Mock<IBasicProperties>();
            _model = new Mock<IModel>();
            _model.Setup(m => m.CreateBasicProperties()).Returns(props.Object);
            _mre = new ManualResetEventSlim(false);
            _model.Setup(m => m.BasicPublish(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<IBasicProperties>(), It.IsAny<byte[]>())).Callback(() => _mre.Set());

        }

        [Test]
        public void BasicSend()
        {
            var connection = new Mock<IConnection>();
            connection.Setup(c => c.CreateModel()).Returns(_model.Object);

            var settings = HarePublisherSettings.GetDefaultSettings();
            settings.IntervalConnectionTries = TimeSpan.Zero;
            settings.MaxMessageWaitingToBeSent = 1;
            var publisher = new RabbitMQHare.RabbitPublisher(settings, new RabbitExchange("testing"))
                {
                    CreateConnection = () => connection.Object,
                    RedeclareMyTopology = model => { }
                };

            publisher.Start();

            Assert.IsTrue(publisher.Started);

            var message = new byte[] {0, 1, 1};
            publisher.Publish("toto", message);

            _mre.Wait(1000);
            _model.Verify(m => m.BasicPublish("testing", "toto", publisher._props, message));
        }
    }
}