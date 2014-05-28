package org.apache.curator.x.rpc.configuration;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.curator.RetryPolicy;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public abstract class RetryPolicyConfiguration
{
    public abstract RetryPolicy build();
}
