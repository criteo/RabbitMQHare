using System.Collections;
using RabbitMQ.Client;

namespace RabbitMQHare
{
    public class RabbitQueue
    {
        //TODO : this class can be modified after giving it to a publisher/consumer,
        //we should have made this a struct instead, or at least use a copy operator

        /// <summary>
        /// A non durable, non exclusive, non auto-delete queue.
        /// </summary>
        /// <param name="name"></param>
        public RabbitQueue(string name)
        {
            Name = name;
            Durable = false;
            Exclusive = false;
            AutoDelete = false;
        }

        /// <summary>
        /// Name used in rabbitmq
        /// </summary>
        public string Name { get; private set; }

        public bool Durable { get; set; }
        public bool Exclusive { get; set; }
        public bool AutoDelete { get; set; }
        public IDictionary Arguments { get; set; }

        /// <summary>
        /// Declare the queue
        /// </summary>
        /// <param name="model"></param>
        public void Declare(IModel model)
        {
            model.QueueDeclare(Name, Durable, Exclusive, AutoDelete, Arguments);
        }
    }
}
