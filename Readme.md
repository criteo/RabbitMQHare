=========
About RabbitMQHare
=========

In a nutshell
-------------

RabbitMQHare is a wrapper around RabbitMQ.Client for C#.

Informations about versions, dependencies, source repositories and contacts can
be found in nuget_.


Need & purpose
--------------

Criteo used to have several rabbitmq-related libs with small code reuse and a lot
of functionnal redundancy with the official client.
Moreover, they lacked customizable failure handling.

Function
--------

We decided to provide a simple wrapper around RabbitMQ.Client_ that:
- provides simple yet powerful customisability
- tries to enforce a light and resilient use of rabbitmq objects

However we do not try to fill every need. Users that require strong insurances,
full featured framework should use another lib or directly the official client.


Limitations
-----------

Edge cases (connection failure for instance) might loose some messages.
You should use this lib if you are ok with loosing some messages.


Example usage
-------------

Publisher:

```
var set = HarePublisherSettings.DefaultSettings;
RabbitPublisher.TemporaryConnectionFailure t = Console.WriteLine;
var random = new Random();
var exchange = new RabbitExchange("carrots");
var initialConnectionTries = 5;

var p = new RabbitPublisher(set, exchange);
p.TemporaryConnectionFailureHandler += t
p.Start(initialConnectionTries);

for(var i=0;i<1000;++i)
    p.Publish(random.Next().ToString(), new byte[0]);
```


Consumer:

```
var set = HareConsumerSettings.DefaultSettings;

var exchange = new RabbitExchange("carrots");
var initialConnectionTries = 5;

BasicDeliverEventHandler messageHandler = (sender, e) =>
{
    try
    {
        Console.WriteLine(e.Body);
    }
    catch (Exception ex)
    {
        Console.WriteLine("request treatment error" + ex);
    }
};
RabbitConnectorCommon.TemporaryConnectionFailure pp = Console.WriteLine;
RabbitConnectorCommon.PermanentConnectionFailure ppp = Console.WriteLine;
var c = new RabbitConsumer(set, exchange);
c.MessageHandler += messageHandler;
c.TemporaryConnectionFailureHandler += pp;
c.PermanentConnectionFailureHandler += ppp;
c.Start(initialConnectionTries);
```


.. _nuget: http://nuget.org/packages/RabbitMQHare
.. _RabbitMQ.Client: http://nuget.org/packages/RabbitMQ.Client
