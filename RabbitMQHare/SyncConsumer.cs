using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQHare
{
    /// <summary>
    /// A class that dispatches messages synchronously.
    ///
    /// That means the OnMessage event should be connected only to things
    /// that return _really_ quickly.
    /// </summary>
    public class SyncConsumer : BaseConsumer
    {
        public SyncConsumer(IModel model, bool autoAck)
            : base(model, autoAck)
        {
            Model.BasicQos(0, 1, false);
        }

        protected override void ProcessOne(BasicDeliverEventArgs e)
        {
            try
            {
                DispatchMessage(e);
                if (AutoAck) Model.BasicAck(e.DeliveryTag, false);
            }
            catch (Exception exception)
            {
                DispatchError(e, exception);
            }
        }
    }
}
