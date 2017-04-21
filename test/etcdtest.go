package main

import (
	"fmt"
	"github.com/bwits/containerfs/utils"
	"os"
	"time"
	"strconv"
	"strings"
	"sync"
)

var wg sync.WaitGroup

func Set(client utils.EtcdV3, a int64) {
	client.Set("/testetcd/"+strconv.FormatInt(a, 10), strconv.FormatInt(a, 10))
	wg.Add(-1)
}
func main() {
	endPoints := strings.Split("127.0.0.1:2379", ",")
	fmt.Println(endPoints)
	client := utils.EtcdV3{}
	err := client.InitEtcd(endPoints)
	if err != nil {
		fmt.Println("connect etcd failed")
		os.Exit(0)
	}

	var a int64

        for i:=0;i<100;i++ {
		for a = 0; a < 10000; a++ {
			wg.Add(1)
			go Set(client, a)
		}
		wg.Wait()
		fmt.Printf("-----------%v----%v\n",i,time.Now())
        }
	/*
		for a = 0; a < 1000; a++ {
			//fmt.Printf("a: %d\n", a)
			go client.Get("/testetcd/"+strconv.FormatInt(a, 10))
		}
	*/
/*
	resp, err := client.GetWithPrefix("/testetcd/999")
	if err != nil {
		fmt.Printf("err:%v\n", err)
	}
	for _, ev := range resp.Kvs {
		fmt.Printf("%s : %s\n", ev.Key, ev.Value)
	}
*/

}
