# Siphon 
===========

Siphon is a hot data sync tools.

Mock slave server, sync data between ssdb master and other server.

Siphon now supports [ssdb](https://github.com/ideawu/ssdb) to [redis](https://github.com/antirez/redis)/[pika](https://github.com/Qihoo360/pika).

* **SYNC** data from master to slave

```sh
siphon sync [--pprof=0.0.0.0:6060] [--ncpu=N]  --f=MASTER    --t=TARGET  [-F masterpassword] [-T targetpassword]
```

Usage:
	siphon sync  [--ncpu=N]  [--parallel=M]   --from=MASTER    --target=TARGET [--frompassword=MASTERPASSWORD] [--targetpassword=SLAVEPASSWORD]

Options:
	--pprof								Set pprof addr and port,like 0.0.0.0:6060 .


Options
-------
+ -n _N_, --ncpu=_N_

> set runtime.GOMAXPROCS to _N_

+ -p _P_, --parallel=_P_

> set redis/pika maximum number of connections to _P_, default is runtime.GOMAXPROCS

+ --pprof=ip:port

> binding pprof on ip:port

Builder
-------
```
go build github.com/imneov/siphon/siphon
```

Example
-------
```
siphon sync  -n 4 -p 4 -f ﻿127.0.0.1:8888 -t 127.0.0.1:9221 -T Stip1234 --pprof=0.0.0.0:6060
```

