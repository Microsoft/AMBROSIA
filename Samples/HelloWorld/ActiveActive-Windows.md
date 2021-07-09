High Availability Using Active Standbys with Hello World
========================================

This walkthrough works with the current code checked into the repostitory, and with released versions of Ambrosia >= 1.0

In Ambrosia, high availability is achieved by running multiple copies of an instance, including one primary which accepts requests, which is responsible for writing the log, and other secondary instances, which remain in recovery, reading any additions to the log made by the primary, until the primary fails. When the primary fails, one of the secondaries becomes the new primary, taking control of all connections to other instances, and also becoming the log writer. All of this is done without modification to the instance code, and with a guarantee of no data loss. Failover times are typically less than a second.

In this walkthrough, we make the server in HelloWorld highly available by running three copies, one of which becomes the primary, and two of which become secondaries. We kill the primary server and observe one of the secondaries becoming primary. This may be repeated to ping-pong the primary responsiblity around. 

This walkthrough assumes that readers have already read [HOWTO-WINDOWS-TwoProc.md](./HOWTO-WINDOWS-TwoProc.md) and [HelloWorldExplained.md](./HelloWorldExplained.md).

Note that, for now, active/active requires 2 process deployments for each immortal. An integrated IC style of running active/active is coming in a future release. 

We begin in a manner very similar to what is described in [HOWTO-WINDOWS-TwoProc.md.md](./HOWTO-WINDOWS-TwoProc.md.md), defining our instances. This time, though, we also logically define replicas for those instances which we will run later:

```bat
cd %AMBROSIATOOLS%\x64\Release\netcoreapp3.1\
dotnet Ambrosia.dll RegisterInstance -i=client -rp=1000 -sp=1001 -l=C:\logs\
dotnet Ambrosia.dll RegisterInstance -i=server -rp=2000 -sp=2001 -l=C:\logs\ -aa
dotnet Ambrosia.dll AddReplica -i=server -rp=3000 -sp=3001 -l=C:\logs\ -r=1
dotnet Ambrosia.dll AddReplica -i=server -rp=4000 -sp=4001 -l=C:\logs\ -r=2
```
Above, we see the two RegisterInstance gestures which define client and server. The only modification is that we added the -aa flag to the server registration call, which indicates that we will add at least one replica. Note that Active/Active requires at least 2 running instances, as all but the first checkpoint will be taken by a secondary, avoiding loss of primary availability during checkpointing.

After the two RegisterInstance gestures, we see two AddReplica gestures for the first and second replicas of server. Note that we are choosing unique receive and send ports for each replica, since we intend to run all four instance runtimes (client, server0, server1, server2) on a single machine. Redundantly, we could add -aa flags to the AddReplica calls, but since all replicas, are by definition associated with active/active deployments, this is redundant.

To run all these, instead of 2 process pairs, we now need 4 (8 processes and console windows in total). To run the server ImmortalCoordinator:

 ```bat
 cd %AMBROSIATOOLS%\x64\Release\netcoreapp3.1
 dotnet ImmortalCoordinator.dll -instanceName=server -port=2500
```

To run the Immortal Coordinator for the first server replica

 ```bat
 cd %AMBROSIATOOLS%\x64\Release\netcoreapp3.1
 dotnet ImmortalCoordinator.dll -instanceName=server -port=3500 -r=1
```

To run the Immortal Coordinator for the second server replica

 ```bat
 cd %AMBROSIATOOLS%\x64\Release\netcoreapp3.1
 dotnet ImmortalCoordinator.dll -instanceName=server -port=4500 -r=2
```

To run the HelloWorld server:

```bat
cd Server\bin\x64\Debug\netcoreapp3.1
dotnet Server.dll server TWOPROC
```

To run the HelloWorld server first replica:

```bat
cd Server\bin\x64\Debug\netcoreapp3.1
dotnet Server.dll server TWOPROC 3001 3000
```

To run the HelloWorld server second replica:

```bat
cd Server\bin\x64\Debug\netcoreapp3.1
dotnet Server.dll server TWOPROC 4001 4000
```

To run the HelloWorld client:

```bat
cd Client1\bin\x64\Debug\netcoreapp3.1
dotnet Client1.dll
```
Like the AddReplica gesture, the server ImmortalCoordinator calls may use a -aa flag, although it's not necessary since this was already established when registering the server instance and its replicas.

To observe failover, CTRL-C the first started server process and its associated immortal coordinator. This should be the primary. Observe that the last started server replica becomes the new primary, which is reflected in the output of the ImmortalCoordinator for that replica. Restart the first server processes. This now becomes an active secondary. You may do this as many times as you like, observing how primary responsibility ping-pongs between the first and third server.

Note that even if we kill both the first and third servers, the second never becomes the primary. The second server has the responsibility of taking checkpoints, and, as a result, is not allowed to become primary. This choice avoids a pathologically bad scenario discovered and written about in:

```bat
Badrish Chandramouli, Jonathan Goldstein:
Shrink - Prescribing Resiliency Solutions for Streaming. PVLDB 10(5): 505-516 (2017)
```

As a result, other than the first copy of an instance ever started, which takes the first checkpoint and runs as primary, if all copies of instances are killed and restarted, they come up the following order:

1) Checkpoint taking secondary, which can never become primary
2) Primary
3) Secondary (which can become primary)
4) Secondary (which can become primary)
5) ...

Note that new secondaries can be registered and started dynamically, allowing Ambrosia to adjust an instance's availability while running.
