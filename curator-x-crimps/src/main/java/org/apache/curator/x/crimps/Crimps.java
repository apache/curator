package org.apache.curator.x.crimps;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Function;

public class Crimps
{
    private final Executor executor;

    private static final Function<CuratorEvent, CrimpResult<String>> nameSupplier = makeSupplier(CuratorEvent::getName);
    private static final Function<CuratorEvent, CrimpResult<String>> pathSupplier = makeSupplier(CuratorEvent::getPath);
    private static final Function<CuratorEvent, CrimpResult<Void>> voidSupplier = makeSupplier(e -> null);
    private static final Function<CuratorEvent, CrimpResult<byte[]>> dataSupplier = makeSupplier(CuratorEvent::getData);
    private static final Function<CuratorEvent, CrimpResult<Stat>> statSupplier = makeSupplier(CuratorEvent::getStat);
    private static final Function<CuratorEvent, CrimpResult<List<String>>> childrenSupplier = makeSupplier(CuratorEvent::getChildren);
    private static final Function<CuratorEvent, CrimpResult<List<ACL>>> aclSupplier = makeSupplier(CuratorEvent::getACLList);

    private static <T> Function<CuratorEvent, CrimpResult<T>> makeSupplier(Function<CuratorEvent, T> proc)
    {
        return event -> (event.getResultCode() == 0) ? new CrimpResult<>(proc.apply(event)) : asException(event);
    }

    private static <T> CrimpResult<T> asException(CuratorEvent event)
    {
        return new CrimpResult<>(KeeperException.create(KeeperException.Code.get(event.getResultCode())));
    }

    public CrimpedBytes<String> nameInBackground(BackgroundPathAndBytesable<String> builder)
    {
        return build(builder, executor, nameSupplier);
    }

    public CrimpedBytes<String> pathInBackground(BackgroundPathAndBytesable<String> builder)
    {
        return build(builder, executor, pathSupplier);
    }

    public Crimped<Void> voidInBackground(BackgroundPathable<Void> builder)
    {
        return build(builder, executor, voidSupplier);
    }

    public Crimped<byte[]> dataInBackground(BackgroundPathable<byte[]> builder)
    {
        return build(builder, executor, dataSupplier);
    }

    public Crimped<List<String>> childrenInBackground(BackgroundPathable<List<String>> builder)
    {
        return build(builder, executor, childrenSupplier);
    }

    public Crimped<Stat> statInBackground(BackgroundPathable<Stat> builder)
    {
        return build(builder, executor, statSupplier);
    }

    public Crimped<List<ACL>> aclsInBackground(BackgroundPathable<List<ACL>> builder)
    {
        return build(builder, executor, aclSupplier);
    }

    public CrimpedBytes<Stat> statBytesInBackground(BackgroundPathAndBytesable<Stat> builder)
    {
        return build(builder, executor, statSupplier);
    }

    public static <T> CrimpedBytes<T> build(BackgroundPathAndBytesable<T> builder, Executor executor, Function<CuratorEvent, CrimpResult<T>> supplier)
    {
        return new CrimpedBytesImpl<>(builder, executor, supplier);
    }

    public static <T> Crimped<T> build(BackgroundPathable<T> builder, Executor executor, Function<CuratorEvent, CrimpResult<T>> supplier)
    {
        return new CrimpedImpl<>(builder, executor, supplier);
    }

    public static Crimps newCrimps()
    {
        return new Crimps(MoreExecutors.sameThreadExecutor());
    }

    public static Crimps newCrimps(Executor executor)
    {
        return new Crimps(executor);
    }

    private Crimps(Executor executor)    // TODO
    {
        this.executor = Objects.requireNonNull(executor, "executor cannot be null");
    }
}
