== Service Discovery Server ==
A REST server for use with Curator Service Discovery. This server can be used for non-Java applications
that need to participate in the Curator Service Discovery.

Full documentation: https://github.com/Netflix/curator/wiki/Service-Discovery-Server


== JSON specifications for REST entities ==

= ServiceInstance =

FIELD               TYPE        REQUIRED    DESCRIPTION
-------------------------------------------------------------------------------------------------------
name                string          Y       Service Name
id                  string          Y       Instance ID
address             string          Y       Hostname/IP
port                int             *       Instance port (port and/or sslPort must be present)
sslPort             int             *       Instance SSL port (port and/or sslPort must be present)
payload             user-defined    N       Instance payload
registrationTimeUTC long            N       Time of the registration in UTC
serviceType         string          Y       Either "STATIC" or "PERMANENT". STATIC will get purged
                                            after the defined threshold has elapsed. PERMANENT must be
                                            manually purged.

Example:
    {
       "name": "test",
       "id": "ca2fff8e-d756-480c-b59e-8297ff88624b",
       "address": "10.20.30.40",
       "port": 1234,
       "payload": "From Test",
       "registrationTimeUTC": 1325129459728,
       "serviceType": "STATIC"
    }

= ServiceInstances =

A list of ServiceInstance entities.

Example:
    [
        {
           "name": "test",
           "id": "ca2fff8e-d756-480c-b59e-8297ff88624b",
           "address": "10.20.30.40",
           "port": 1234,
           "payload": "From Test",
           "registrationTimeUTC": 1325129459728,
           "serviceType": "STATIC"
        },

        {
           "name": "foo",
           "id": "bd4fff8e-c234-480c-f6ee-8297ff813765",
           "address": "10.20.30.40",
           "sslPort": 1235,
           "payload": "foo-bar",
           "registrationTimeUTC": 1325129459728,
           "serviceType": "STATIC"
        }
    ]

= ServiceNames =

A list of strings (service names).

Example:
    [
        {
            "name": "foo"
        },

        {
            "name": "bar"
        }
    ]
