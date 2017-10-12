// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"runtime"
	"strconv"
	"github.com/docopt/docopt-go"
	"github.com/reborndb/go/errors"
	"github.com/reborndb/go/log"
	"fmt"
	"encoding/binary"
	"goim/libs/perf"
)


var  args struct {
	parallel int

	from   string
	target string

	fromAuth string
	targetAuth string
}

func  parseIntFromString(s string, min, max int) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	if n >= min && n <= max {
		return n, nil
	}
	return 0, errors.Errorf("out of range [%d,%d], got %d", min, max, n)
}


func main() {
	usage := `
Usage:
	siphon sync [--pprof=0.0.0.0:6060] [--ncpu=N]  [--parallel=M]   --from=MASTER    --target=TARGET [--frompassword=MASTERPASSWORD] [--targetpassword=SLAVEPASSWORD]

Options:
	--pprof								Set pprof addr and port,like 0.0.0.0:6060 .
	-n N, --ncpu=N                    	Set runtime.GOMAXPROCS to N .
	-p M, --parallel=M                	Set the number of parallel routines to M .
	-f MASTER, --from=MASTER          	Set host:port of master .
	-t TARGET, --target=TARGET        	Set host:port of target .
	-F MASTERPASSWORD, --frompassword	Set password of master .
	-T SLAVEPASSWORD, --targetpassword	Set password of target .
`

	i :=  int64(-3)

	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i))
	fmt.Println(b)

	d, err := docopt.Parse(usage, nil, true, "", false)
	if err != nil {
		log.PanicError(err, "parse arguments failed")
	}

	if s, ok := d["--ncpu"].(string); ok && s != "" {
		n, err :=  parseIntFromString(s, 1, 1024)
		if err != nil {
			log.PanicErrorf(err, "parse --ncpu failed")
		}
		runtime.GOMAXPROCS(n)
	}
	ncpu := runtime.GOMAXPROCS(0)

	if s, ok := d["--parallel"].(string); ok && s != "" {
		n, err :=  parseIntFromString(s, 1, 1024)
		if err != nil {
			log.PanicErrorf(err, "parse --parallel failed")
		}
		args.parallel = n
	}
	if ncpu > args.parallel {
		args.parallel = ncpu
	}
	if args.parallel == 0 {
		args.parallel = 4
	}

	args.target, _ = d["--target"].(string)
	args.from, _ = d["--from"].(string)

	args.fromAuth, _ = d["--frompassword"].(string)
	args.targetAuth, _ = d["--targetpassword"].(string)

	log.Infof("set ncpu = %d, parallel = %d\n", ncpu, args.parallel)



	pprofAddr, _ := d["--pprof"].(string)
	if pprofAddr != ""{
		log.Infof("init pprof on  %s\n", pprofAddr)
		InitPerf([]string{pprofAddr})
	}

	switch {
	case d["sync"].(bool):
		new(cmdSync).Main()
	}
}


