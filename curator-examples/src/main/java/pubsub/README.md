# Pub-Sub Example
This example models a publish and subscribe system (note: it is not meant for production) using the strongly typed modeled APIs in Apache Curator. 

## Design Notes

In this example, there are three models that can be published: `Instance`, `LocationAvailable` and `UserCreated`. Instances have an `InstanceType`; LocationAvailable and UserCreated both have a `Priority` and are associated with a `Group`. (Note: these names/objects are meant for illustrative purposes only and are completely contrived)

Each model is stored at a unique path in ZooKeeper:

* Instance: `/root/pubsub/instances/TYPE/ID`
* LocationAvailable: `/root/pubsub/messages/locations/GROUP/PRIORITY/ID`
* UserCreated: `/root/pubsub/messages/users/GROUP/PRIORITY/ID`

All models are stored using a TTL so that they automatically get deleted after 10 minutes.

## Clients, Models and Paths

This example uses the "typed" models (`TypedModelSpec`, etc.). The typed paths, models and clients are meant to be created early in your application and re-used as needed. Thus, you can model your ZooKeeper usage and the rest of your application can use them without worrying about correct paths, types, etc.

In the Pub-Sub example, the paths are defined in `Paths.java`, the model specs are defined in `ModelSpecs.java` and the client templates are defined in `Clients.java`.

### TypedZPath

`TypedZPath` is a template that produces a `ZPath` by applying parameters. Curator provides variants that accept from 1 to 10 parameters (`TypedZPath`, `TypedZPath2`, `TypedZPath3`, etc.).

In this example, the TypedZPaths are defined in `Paths.java`. E.g.

```
public static final TypedZPath2<Group, Priority> locationAvailablePath = 
    TypedZPath2.from(basePath + "/messages/locations/{id}/{id}");

```

This creates a TypedZPath that requires two parameters, a `Group` and a `Priority`. When the `resolved()` method is called with a group and priority, the "{id}" values are replaced in order.

### TypedModelSpec

`TypedModelSpec` is a template that produces a `ModelSpec` by applying parameters to the contained `TypedZPath`. Curator provides variants that accept from 1 to 10 parameters (`TypedModelSpec`, `TypedModelSpec2`, `TypedModelSpec3`, etc.).

In this example, the TypedModelSpecs are defined in `ModelSpecs.java`. E.g.

```
public static final TypedModelSpec<Instance, InstanceType> instanceModelSpec = 
    TypedModelSpec.from(builder(Instance.class), Paths.instancesPath);
```

The `builder()` method creates a ModelSpec builder. 

### TypedModeledFramework

`TypedModeledFramework` is a template that produces a `ModeledFramework` by applying parameters to the `TypedZPath` in the contained `TypedModelSpec`. Curator provides variants that accept from 1 to 10 parameters (`TypedModeledFramework`, `TypedModeledFramework2`, `TypedModeledFramework3`, etc.).

In this example, the TypedModelSpecs are defined in `Clients.java`. E.g.

```
public static final TypedModeledFramework<Instance, InstanceType> instanceClient = 
   TypedModeledFramework.from(ModeledFramework.builder(), ModelSpecs.instanceModelSpec)
```
