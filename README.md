# Siphon 
===========

Siphon is a hot data sync tools.

Mock slave server, sync data between ssdb master and other server.

Siphon now supports ssdb to redis/pika.

* **SYNC** data from master to slave

```sh
siphongo sync     [--ncpu=N]   --f=MASTER    --t=TARGET  [-F masterpassword] [-T targetpassword]
```

Options
-------
+ -n _N_, --ncpu=_N_

> set runtime.GOMAXPROCS to _N_

Builder
-------
```
go build github.com/imneov/siphon/siphon
```

Example
-------
```
siphon sync -f ﻿127.0.0.1:8888 -t 127.0.0.1:9221 -n 4 -T Stip1234
```