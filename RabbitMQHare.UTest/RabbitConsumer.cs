using System;
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
                context.Consumer.Start();

                Assert.IsTrue(context.Consumer.HasAlreadyStartedOnce);

                context.Consumer._myConsumer.HandleBasicDeliver("toto", 1, false, "testing", "routingKey", new BasicProperties(), new byte[] { 0, 1, 0 });

                Assert.AreEqual(1, received);

                if (failInHandler)
                    Assert.AreEqual(1, error);
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

                context.Consumer.Start();

                Assert.IsTrue(context.Consumer.HasAlreadyStartedOnce);

                var restarted = 0;
                context.Consumer.StartHandler += (_, e) => ++restarted;

                context.Consumer._myConsumer.HandleModelShutdown(context.Model.Object, new ShutdownEventArgs(ShutdownInitiator.Peer, 0, "Thanks for playing"));

                Assert.AreEqual(1, restarted);
                context.Consumer._myConsumer.HandleBasicDeliver("toto", 1, false, "testing", "routingKey", new BasicProperties(), new byte[] { 0, 1, 0 });
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