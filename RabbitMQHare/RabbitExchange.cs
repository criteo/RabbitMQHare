using System.Collections;
using RabbitMQ.Client;

namespace RabbitMQHare
{
    public class RabbitExchange
    {
        //TODO : this class can be modified after giving it to a publisher/consumer,
        //we should have made this a struct instead, or at least use a copy operator 

        public RabbitExchange(string name)
        {
            Name = name;
            Type = ExchangeType.Fanout;
            Durable = false;
            AutoDelete = true;
        }

        public string Name { get; private set; }
        public string Type { get; set; }
        public bool Durable { get; set; }
        public bool AutoDelete { get; set; }
        public IDictionary Arguments { get; set; }

        /// <summary>
        /// Declare the exchange against a model
        /// </summary>
        /// <param name="model"></param>
        public void Declare(IModel model)
        {
            model.ExchangeDeclare(Name, Type, Durable, AutoDelete, Arguments);
        }
    }
}
