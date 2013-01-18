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

Criteo used to have several rabbitmq-related libs small code reuse, a lot of 
functionnal redundancy with the official client.
Moreover, they lacked of customizable failure handling.
However we do not try to fill every need. Users that require strong insurances,
full featured framework should use another lib or directly the official client.


Function
--------

We decided to provide a simple wrapper around RabbitMQ.Client_ that:
- provide simple yet powerful customisatability
- try to enforce a light and resilient use of rabbitmq objects

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
var exchange = new RabbitExchange("test323232");

var p = new RabbitPublisher(set, exchange);
p.TemporaryConnectionFailureHandler += t
p.Start();

for(var i=0;i<1000;++i)
    p.Publish(random.Next().ToString(), new byte[0]);
```

Consumer:

```
var set = HareConsumerSettings.DefaultSettings;

var random = new Random();
var exchange = new RabbitExchange("test323232");

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
RabbitConnectorCommon.TemporaryConnectionFailure pp = (t) => Console.WriteLine(t);
RabbitConnectorCommon.PermanentConnectionFailure ppp = (t) => Console.WriteLine(t);
var c = new RabbitConsumer(set, exchange);
c.MessageHandler += messageHandler;
c.TemporaryConnectionFailureHandler += pp;
c.PermanentConnectionFailureHandler += ppp;
c.Start();
```


.. _nuget: http://nuget.org/packages/RabbitMQHare
.. _RabbitMQ.Client: http://nuget.org/packages/RabbitMQ.Client