package org.apache.curator.x.async;

import org.apache.zookeeper.data.Stat;
import java.util.List;

public interface AsyncReconfigBuilder
{
    /**
     * Sets one or more members that are meant to be the ensemble.
     * The expected format is server.[id]=[hostname]:[peer port]:[election port]:[type];[client port]
     *
     * @param servers The servers joining.
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers);

    AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving);

    /**
     * Sets one or more members that are meant to be the ensemble.
     * The expected format is server.[id]=[hostname]:[peer port]:[election port]:[type];[client port]
     *
     * @param servers The servers joining.
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, long fromConfig);

    AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving, long fromConfig);

    /**
     * Sets one or more members that are meant to be the ensemble.
     * The expected format is server.[id]=[hostname]:[peer port]:[election port]:[type];[client port]
     *
     * @param servers The servers joining.
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, Stat stat);

    AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving, Stat stat);

    /**
     * Sets one or more members that are meant to be the ensemble.
     * The expected format is server.[id]=[hostname]:[peer port]:[election port]:[type];[client port]
     *
     * @param servers The servers joining.
     * @return this
     */
    AsyncEnsemblable<AsyncStage<Void>> withNewMembers(List<String> servers, Stat stat, long fromConfig);

    AsyncEnsemblable<AsyncStage<Void>> withJoiningAndLeaving(List<String> joining, List<String> leaving, Stat stat, long fromConfig);
}
