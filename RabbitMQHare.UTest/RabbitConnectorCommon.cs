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

                testContext.Stub.InternalStart();

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
                connection.Setup(c => c.CreateModel()).Returns(testContext.Model.Object).Callback(() => { if (++calls < 3) throw new Exception("Argh I fail to connect !"); });

                testContext.Stub = new StubConnectorCommon(testContext.Conf.Object)
                    {
                        //silently replace the connection getter
                        CreateConnection = () => connection.Object,
                        RedeclareMyTopology = _ => { },
                    };

                testContext.Stub.InternalStart();

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