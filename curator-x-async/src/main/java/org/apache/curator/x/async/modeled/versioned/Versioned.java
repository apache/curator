package org.apache.curator.x.async.modeled.versioned;

/**
 * A container for a model instance and a version. Can be used with the
 * {@link org.apache.curator.x.async.modeled.ModeledFramework#versioned()} APIs
 */
@FunctionalInterface
public interface Versioned<T>
{
    /**
     * Returns the contained model
     *
     * @return model
     */
    T model();

    /**
     * Returns the version of the model when it was read
     *
     * @return version
     */
    default int version()
    {
        return -1;
    }

    /**
     * Return a new Versioned wrapper for the given model and version
     *
     * @param model model
     * @param version version
     * @return new Versioned wrapper
     */
    static <T> Versioned<T> from(T model, int version)
    {
        return new Versioned<T>()
        {
            @Override
            public int version()
            {
                return version;
            }

            @Override
            public T model()
            {
                return model;
            }
        };
    }
}
