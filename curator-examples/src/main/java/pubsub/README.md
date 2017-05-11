# Pub-Sub Example
This example models a publish and subscribe system (note: it is not meant for production) using 
the strongly typed modeled APIs in Apache Curator. 

## Design Notes

In this example, there are three models that can be published: `Instance`, `LocationAvailable` 
and `UserCreated`. Instances have an `InstanceType`; LocationAvailable and UserCreated both have 
a `Priority` and are associated with a `Group`. (Note: these names/objects are meant for 
illustrative purposes only and are completely contrived)

Each model is stored at a unique path in ZooKeeper:

* Instance: `/root/pubsub/instances/TYPE/ID`
* LocationAvailable: `/root/pubsub/messages/locations/GROUP/PRIORITY/ID`
* UserCreated: `/root/pubsub/messages/users/GROUP/PRIORITY/ID`

All models are stored using a TTL so that they automatically get deleted after 10 minutes.

## Clients

This example uses the "typed" models (`TypedModelSpec`, etc.). The typed paths, models and 
clients are meant to be created early in your application and re-used as needed. Thus, you 
can model your ZooKeeper usage and the rest of your application can use them without worrying 
about correct paths, types, etc.

`TypedModeledFramework` is a template that produces a `ModeledFramework` by applying 
parameters to the `TypedZPath` in the contained `TypedModelSpec`. Curator provides variants 
that accept from 1 to 10 parameters (`TypedModeledFramework`, `TypedModeledFramework2`, 
`TypedModeledFramework3`, etc.).

In this example, the TypedModeledFrameworks are defined in `Clients.java`. E.g.

```
public static final TypedModeledFramework2<LocationAvailable, Group, Priority> locationAvailableClient = 
    TypedModeledFramework2.from(
        ModeledFramework.builder(),
        builder(LocationAvailable.class),
        "/root/pubsub/messages/locations/{group}/{priority}"
    );
```

## Publisher

`Publisher.java` shows how to use the ModeledFramework to write models. There are methods to 
write single instances and to write lists of instances in a transaction. Each publish method 
resolves the appropriate typed client and then calls its `set()` method with the given model.

## Subscriber

`Subscriber.java` uses CachedModeledFrameworks to listen for changes on the parent nodes for 
all of the models in this example. Each of the methods resolves the appropriate typed client 
and then starts the cache (via `cached()`).

## SubPubTest

`SubPubTest.java` is a class that exercises this example. 

* `start()` uses `Subscriber` to start a `CachedModeledFramework` for each combination of 
the Instance + InstanceType, LocationAvailable + Group + Priority, and UserCreated + Group + Priority. It then adds a simple listener to each cache that merely prints the class name 
and path whenever an update occurs (see `generalListener()`).
* `start()` also starts a scheduled task that runs every second. This task calls 
`publishSomething()`
* `publishSomething()` randomly publishes either a single Instance, LocationAvailable, 
UserCreated or a list of those.

`SubPubTest.java` has a `main()` method. When you run you should see something similar to this:

```
Publishing 9 instances
Subscribed Instance @ /root/pubsub/instances/proxy/1
Subscribed Instance @ /root/pubsub/instances/web/2
Subscribed Instance @ /root/pubsub/instances/cache/4
Subscribed Instance @ /root/pubsub/instances/proxy/9
Subscribed Instance @ /root/pubsub/instances/database/3
Subscribed Instance @ /root/pubsub/instances/cache/5
Subscribed Instance @ /root/pubsub/instances/database/6
Subscribed Instance @ /root/pubsub/instances/cache/7
Subscribed Instance @ /root/pubsub/instances/cache/8
Publishing 1 userCreated
Subscribed UserCreated @ /root/pubsub/messages/users/main/high/10
Publishing 9 locationsAvailable
Subscribed LocationAvailable @ /root/pubsub/messages/locations/admin/low/11
Subscribed LocationAvailable @ /root/pubsub/messages/locations/admin/medium/12
Subscribed LocationAvailable @ /root/pubsub/messages/locations/admin/medium/13
Subscribed LocationAvailable @ /root/pubsub/messages/locations/admin/medium/14
Subscribed LocationAvailable @ /root/pubsub/messages/locations/admin/medium/16
Subscribed LocationAvailable @ /root/pubsub/messages/locations/admin/high/15
Subscribed LocationAvailable @ /root/pubsub/messages/locations/admin/medium/17
...
```

It runs for 1 minute and then exits.
