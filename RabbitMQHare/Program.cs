using System;
using RabbitMQ.Client.Events;

namespace RabbitMQHare
{
    class Program
    {
        static void Main(string[] args)
        {
            var queue = new RabbitQueue("myQueue");

            var pubSettings = HarePublisherSettings.DefaultSettings;
            pubSettings.ConnectionFactory.HostName = "10.50.1.5";
            pubSettings.ConnectionFactory.VirtualHost = "Test";

            RabbitMQHare.RabbitConnectorCommon.TemporaryConnectionFailure temp = e => Console.WriteLine(e.ToString());
            RabbitMQHare.RabbitConnectorCommon.PermanentConnectionFailure perm = e => Console.WriteLine(e.ToString());

            using (var p = new RabbitPublisher(pubSettings, queue, temp,perm))
            {
                Console.WriteLine("Starting");
                p.Start();
                for (var i = 0; i < 10; i++)
                    p.Publish("titi", new byte[3] { 0, 1, 5 });
                Console.WriteLine("Waiting for key to stop");
                Console.ReadKey();
            }

            var consSettings = HareConsumerSettings.DefaultSettings;
            consSettings.ConnectionFactory.HostName = "10.50.1.5";
            consSettings.ConnectionFactory.VirtualHost = "Test";
            ConsumerEventHandler stop = (e, o) => Console.WriteLine(o.ConsumerTag + " will stop");
            CallbackExceptionEventHandler excep =  (_, e)=> Console.WriteLine("error " + e.ToString());
            BasicDeliverEventHandler messageHandler = (_, e) => Console.WriteLine(e.Body);


            using (var c = new RabbitConsumer(consSettings, queue, temp, perm, null,stop, excep, messageHandler))
            {
                Console.WriteLine("Starting");
                c.Start();
                Console.WriteLine("Waiting for key to stop");
                Console.ReadKey();
                Console.WriteLine("Will stop");
            }

            Console.WriteLine("stopped");
            

        }
    }
}
