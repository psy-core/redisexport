package main

import (
	"flag"
	"fmt"
	"github.com/FZambia/sentinel"
	"github.com/gomodule/redigo/redis"
	"os"
	"strconv"
	"strings"
)

var scancount int = 0
var processcount int = 0

var PREFIX string

func main() {
	h := flag.Bool("h", false, "show this help")
	s := flag.Bool("s", false, "sentinel mode, default is false")
	db := flag.Int("db", 0, "db name, 0-15")
	passwd := flag.String("passwd", "", "redis password")
	prefix := flag.String("prefix", "", "key prefix")
	flag.Usage = usage
	flag.Parse()

	if len(flag.Args()) < 1 {
		flag.Usage()
		return
	}
	masteraddr := flag.Args()[0]

	if *h {
		flag.Usage()
		return
	}

	PREFIX = *prefix

	if *s {
		sntnl := &sentinel.Sentinel{
			Addrs: []string{os.Args[1]},
			Dial: func(addr string) (redis.Conn, error) {
				c, err := redis.Dial("tcp", masteraddr, redis.DialDatabase(*db), redis.DialPassword(*passwd))
				if err != nil {
					fmt.Println("redis dial error: " + err.Error())
					return nil, err
				}
				return c, nil
			},
		}

		var err error
		masteraddr, err = sntnl.MasterAddr()
		if err != nil {
			fmt.Println("redis sentinel get master addr error:" + err.Error())
			return
		}
	}

	c, err := redis.Dial("tcp", masteraddr, redis.DialDatabase(*db), redis.DialPassword(*passwd))
	if err != nil {
		fmt.Printf("redis dial master addr %v error: %v\n", masteraddr, err.Error())
		return
	}

	defer c.Close()

	cursor := processOneScan(c, 0)
	for cursor > 0 {
		cursor = processOneScan(c, cursor)
	}

	//fmt.Printf("finished. scancount:%v, processcount:%v\n", scancount, processcount)
}

func processOneScan(c redis.Conn, index int) int {
	r, err := redis.Values(c.Do("SCAN", index))
	if err != nil {
		fmt.Println("redis scan error: " + err.Error())
		return 0
	}

	cursor, _ := strconv.Atoi(string(r[0].([]uint8)))

	for _, v := range r[1].([]interface{}) {
		key := string(v.([]uint8))
		if PREFIX != "" && !strings.HasPrefix(key, PREFIX) {
			continue
		}
		processOneKey(c, key)
	}

	return cursor
}

func processOneKey(c redis.Conn, key string) {

	scancount ++
	if scancount%10000 == 0 {
		fmt.Printf("scan count :%v\n", scancount)
	}
	r, err := redis.String(c.Do("GET", key))
	if err != nil {
		fmt.Printf("redis get %v error: %v\n", key, err.Error())
		return
	}
	fmt.Printf("%s \t\t\t\t %s\n", key, r)
	processcount ++
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: redisexport [-s] [-d db] [-p passwd] [-h] <ip:port>
Options:
`)
	flag.PrintDefaults()
}
