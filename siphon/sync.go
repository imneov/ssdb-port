// Copyright 2015 Reborndb Org. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"time"

	"github.com/reborndb/go/atomic2"
	"github.com/reborndb/go/log"
	redis "github.com/garyburd/redigo/redis"
	ssdb "github.com/imneov/siphon/ssdb"
	"fmt"
	"runtime"
)

var (
	pool          *redis.Pool
)

type cmdSync struct {
	nread, nrecv, nobjs atomic2.Int64
}

func (cmd *cmdSync) Main() {
	from, fromAuth, target, targetAuth := args.from, args.fromAuth, args.target, args.targetAuth

	if len(from) == 0 {
		log.Panic("invalid argument: from")
	}
	if len(target) == 0 {
		log.Panic("invalid argument: target")
	}

	log.Infof("sync from '%s' to '%s'\n", from, target)

	cmdsQueue := make(chan []string, 1000)

	pool = newPool(target, targetAuth)
	for i:=0;i< args.parallel;i++ {
		go func(){
			for{
				cmd := <- cmdsQueue
				//log.Info("cmd: (%v)", cmd)
				sendCmd(pool, cmd)
			}
		}()
	}

	if server, err := ssdb.NewSSDBSalve(from, fromAuth, &cmdsQueue); err == nil {
		server.Start()
	}

	runtime.Gosched()

	return
}


//初始化一个pool
func newPool(target, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     4,
		MaxActive:   1024,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", target,
				redis.DialConnectTimeout(5 * time.Second),
				redis.DialReadTimeout(5 * time.Second),
				redis.DialWriteTimeout(5 * time.Second),
			)
			if err != nil {
				return nil, err
			}

			if password != "" {
				status, err := c.Do("AUTH", password)
				if  err != nil {
					c.Close()
					return nil, err
				}
				fmt.Println("AUTH",status)
			}

			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func sendCmd(pool *redis.Pool, cmd []string) (err error){

	if len(cmd) < 1 {
		return nil
	}
	conn := pool.Get()
	defer conn.Close()
	//redis操作
	commandName := cmd[0]
	arguments := []interface{}{}
	for _, command := range cmd[1:] {
		arguments = append(arguments, command)
	}
	_, err = conn.Do(commandName,arguments...)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return err
}
