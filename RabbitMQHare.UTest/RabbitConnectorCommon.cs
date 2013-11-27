using System;
using Moq;
using NUnit.Framework;
using RabbitMQ.Client;

namespace RabbitMQHare.UTest
{
    class RabbitConnectorCommon
    {
        private Mock<IModel> _model;
        private Mock<IHareSettings> _conf;

        [SetUp]
        public void Setup()
        {
            _model = new Mock<IModel>();
            _conf = new Mock<IHareSettings>();
            _conf.Setup(a => a.MaxConnectionRetry).Returns(5);
        }

        [Test]
        public void BasicTest()
        {
            var connection = new Mock<IConnection>();
            connection.Setup(c => c.CreateModel()).Returns(_model.Object);

            var stub = new StubConnectorCommon(_conf.Object)
                {
                    //silently replace the connection getter
                    CreateConnection = () => connection.Object,
                    RedeclareMyTopology = _ => { },
                };

            stub.InternalStart();

            Assert.IsTrue(stub.HasAlreadyStartedOnce);
        }

        [Test]
        public void RelunctantConnectivity()
        {
            var connection = new Mock<IConnection>();
            var calls = 0;
            connection.Setup(c => c.CreateModel()).Returns(_model.Object).Callback(() => { if (++calls < 3) throw new Exception("Argh I fail to connect !"); });

            var stub = new StubConnectorCommon(_conf.Object)
                {
                    //silently replace the connection getter
                    CreateConnection = () => connection.Object,
                    RedeclareMyTopology = _ => { },
                };

            stub.InternalStart();

            Assert.IsTrue(stub.HasAlreadyStartedOnce);
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