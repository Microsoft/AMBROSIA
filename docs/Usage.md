---
layout: default
title: Usage
nav_order: 3
---
## Usage
```
Usage: Ambrosia.exe RegisterInstance [OPTIONS]
Options:
  -i, --instanceName=VALUE   The instance name [REQUIRED].
      --rp, --receivePort=VALUE
                             The service receive from port [REQUIRED].
      --sp, --sendPort=VALUE The service send to port. [REQUIRED]
  -l, --log=VALUE            The service log path.
      --cs, --createService=VALUE
                             [A - AutoRecovery | N - NoRecovery | Y -
                               AlwaysRecover].
      --ps, --pauseAtStart   Is pause at start enabled.
      --npl, --noPersistLogs Is persistent logging disabled.
      --lts, --logTriggerSize=VALUE
                             Log trigger size (in MBs).
      --aa, --activeActive   Is active-active enabled.
      --cv, --currentVersion=VALUE
                             The current version #.
      --uv, --upgradeVersion=VALUE
                             The upgrade version #.
  -h, --help                 show this message and exit
Usage: Ambrosia.exe AddReplica [OPTIONS]
Options:
  -r, --replicaNum=VALUE     The replica # [REQUIRED].
  -i, --instanceName=VALUE   The instance name [REQUIRED].
      --rp, --receivePort=VALUE
                             The service receive from port [REQUIRED].
      --sp, --sendPort=VALUE The service send to port. [REQUIRED]
  -l, --log=VALUE            The service log path.
      --cs, --createService=VALUE
                             [A - AutoRecovery | N - NoRecovery | Y -
                               AlwaysRecover].
      --ps, --pauseAtStart   Is pause at start enabled.
      --npl, --noPersistLogs Is persistent logging disabled.
      --lts, --logTriggerSize=VALUE
                             Log trigger size (in MBs).
      --aa, --activeActive   Is active-active enabled.
      --cv, --currentVersion=VALUE
                             The current version #.
      --uv, --upgradeVersion=VALUE
                             The upgrade version #.
  -h, --help                 show this message and exit
Usage: Ambrosia.exe DebugInstance [OPTIONS]
Options:
  -i, --instanceName=VALUE   The instance name [REQUIRED].
      --rp, --receivePort=VALUE
                             The service receive from port [REQUIRED].
      --sp, --sendPort=VALUE The service send to port. [REQUIRED]
  -l, --log=VALUE            The service log path.
  -c, --checkpoint=VALUE     The checkpoint # to load.
      --cv, --currentVersion=VALUE
                             The version # to debug.
      --tu, --testingUpgrade Is testing upgrade.
  -h, --help                 show this message and exit
```