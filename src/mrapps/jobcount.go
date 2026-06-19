package main

//
// a MapReduce pseudo-application that counts the number of times map/reduce
// tasks are run, to test whether jobs are assigned multiple times even when
// there is no failure.
//
// go build -buildmode=plugin crash.go
//

import "6.5840/mr"
import "math/rand"
import "strings"
import "strconv"
import "time"
import "fmt"
import "os"


var count int

// 用来count变量统计map被调用了多少次，每被调用一次就往一个文件里写入一个x，并返回a，x
func Map(filename string, contents string) []mr.KeyValue {
	me := os.Getpid()
	f := fmt.Sprintf("mr-worker-jobcount-%d-%d", me, count)
	count++
	err := os.WriteFile(f, []byte("x"), 0666)
	if err != nil {
		panic(err)
	}
	time.Sleep(time.Duration(2000+rand.Intn(3000)) * time.Millisecond)
	return []mr.KeyValue{mr.KeyValue{"a", "x"}}
}

// 统计有多少个文件
func Reduce(key string, values []string) string {
	files, err := os.ReadDir(".")
	if err != nil {
		panic(err)
	}
	invocations := 0
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "mr-worker-jobcount") {
			invocations++
		}
	}
	return strconv.Itoa(invocations)
}
