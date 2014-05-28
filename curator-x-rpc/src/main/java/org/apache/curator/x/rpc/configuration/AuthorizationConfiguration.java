package org.apache.curator.x.rpc.configuration;

public class AuthorizationConfiguration
{
    private String scheme;
    private String auth;

    public String getScheme()
    {
        return scheme;
    }

    public void setScheme(String scheme)
    {
        this.scheme = scheme;
    }

    public String getAuth()
    {
        return auth;
    }

    public void setAuth(String auth)
    {
        this.auth = auth;
    }
}
