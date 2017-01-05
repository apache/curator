package org.apache.curator.x.crimps;

import org.apache.curator.framework.api.BackgroundPathAndBytesable;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import java.util.List;
import java.util.function.Function;

public class Crimps
{
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

    public static CrimpedBytes<String> nameInBackground(BackgroundPathAndBytesable<String> builder)
    {
        return build(builder, nameSupplier);
    }

    public static CrimpedBytes<String> pathInBackground(BackgroundPathAndBytesable<String> builder)
    {
        return build(builder, pathSupplier);
    }

    public static Crimped<Void> voidInBackground(BackgroundPathable<Void> builder)
    {
        return build(builder, voidSupplier);
    }

    public static Crimped<byte[]> dataInBackground(BackgroundPathable<byte[]> builder)
    {
        return build(builder, dataSupplier);
    }

    public static Crimped<List<String>> childrenInBackground(BackgroundPathable<List<String>> builder)
    {
        return build(builder, childrenSupplier);
    }

    public static Crimped<Stat> statInBackground(BackgroundPathable<Stat> builder)
    {
        return build(builder, statSupplier);
    }

    public static Crimped<List<ACL>> aclsInBackground(BackgroundPathable<List<ACL>> builder)
    {
        return build(builder, aclSupplier);
    }

    public static CrimpedBytes<Stat> statBytesInBackground(BackgroundPathAndBytesable<Stat> builder)
    {
        return build(builder, statSupplier);
    }

    public static <T> CrimpedBytes<T> build(BackgroundPathAndBytesable<T> builder, Function<CuratorEvent, CrimpResult<T>> supplier)
    {
        return new CrimpedBytesImpl<>(builder, supplier);
    }

    public static <T> Crimped<T> build(BackgroundPathable<T> builder, Function<CuratorEvent, CrimpResult<T>> supplier)
    {
        return new CrimpedImpl<>(builder, supplier);
    }

    private Crimps()
    {
    }
}
