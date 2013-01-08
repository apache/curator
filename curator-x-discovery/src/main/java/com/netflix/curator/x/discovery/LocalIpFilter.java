package com.netflix.curator.x.discovery;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;

public interface LocalIpFilter
{
    public boolean      use(NetworkInterface networkInterface, InetAddress address) throws SocketException;
}
