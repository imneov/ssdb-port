// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"runtime"
	"strconv"
	"github.com/docopt/docopt-go"
	"github.com/reborndb/go/bytesize"
	"github.com/reborndb/go/errors"
	"github.com/reborndb/go/log"
)


var  args struct {
	parallel int

	from   string
	target string

	fromAuth string
	targetAuth string
}
const (
	ReaderBufferSize = bytesize.MB * 32
	WriterBufferSize = bytesize.MB * 8
)

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

const (
	MinDB = 0
	MaxDB = 1023
)

var acceptDB = func(db uint32) bool {
	return db >= MinDB && db <= MaxDB
}

func main() {
	usage := `
Usage:
	siphon sync [--ncpu=N]  [--parallel=M]   --from=MASTER    --target=TARGET [--frompassword=MASTERPASSWORD] [--targetpassword=SLAVEPASSWORD]

Options:
	-n N, --ncpu=N                    Set runtime.GOMAXPROCS to N.
	-p M, --parallel=M                Set the number of parallel routines to M.
	-f MASTER, --from=MASTER          Set host:port of master .
	-t TARGET, --target=TARGET        Set host:port of slave .
	-F MASTERPASSWORD, --frompassword	Set password of master .
	-T SLAVEPASSWORD, --targetpassword	Set password of target .
`
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
	args.fromAuth, _ = d["--targetpassword"].(string)

	log.Infof("set ncpu = %d, parallel = %d\n", ncpu, args.parallel)

	switch {
	case d["sync"].(bool):
		new(cmdSync).Main()
	}
}
