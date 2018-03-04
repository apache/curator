package org.apache.curator.x.discovery;


/**
 * Constructs ZK paths to services for service discovering
 */
public interface DiscoveryPathConstructor
{
    /**
     * Return the path where all service names registered
     * @return basePath
     */
    String getBasePath();

    /**
     * Return the path where all instances of service registered
     * @param name service name
     * @return path to service instances
     */
    String getPathForInstances(String name);

    /**
     * Return the path where concrete instance registered
     * @param name service name
     * @param id concrete instance id
     * @return path to concrete instance
     */
    String getPathForInstance(String name, String id);
}
