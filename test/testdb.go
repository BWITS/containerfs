package main

import (
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	//"os"
	_ "github.com/go-sql-driver/mysql"
	"github.com/bwits/containerfs/logger"
	"math/rand"
	"reflect"
	"unsafe"
)

func init() {
	logger.SetConsole(true)
	logger.SetRollingFile("/tmp/", "test.log", 10, 5, logger.MB)
	logger.SetLevel(logger.DEBUG)

}

func insert(db *sql.DB) {
	//_,err := db.Exec("lock tables info write")

	stmt, err := db.Prepare("INSERT INTO info(name, age) VALUES(?, ?)")
	defer stmt.Close()

	if err != nil {
		//log.Println(err)
		return
	}

	for i := 0; i < 100; i++ {
		if i < 25 {
			stmt.Exec("jls", strconv.Itoa(i))
		} else if i < 50 {
			stmt.Exec("zxc", strconv.Itoa(i))
		} else if i < 75 {
			stmt.Exec("hjf", strconv.Itoa(i))
		} else {
			stmt.Exec("wxg", strconv.Itoa(i))
		}
	}
	//db.Exec("unlock tables")
}

func convert(b []byte) string {
	s := make([]string, len(b))
	for i := range b {
		s[i] = strconv.Itoa(int(b[i]))
	}
	return strings.Join(s, ",")
}

func B2S(buf []byte) string {
	return *(*string)(unsafe.Pointer(&buf))
}

func S2B(s *string) []byte {
	return *(*[]byte)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(s))))
}

func rand_test() chan int {
	out := make(chan int)
	rand.Seed(int64(time.Now().Nanosecond())) // set time, otherwise each the same randint value
	go func() {
		for {
			out <- rand.Int()
		}
	}()
	return out
}

func main() {
	//var ss string
	//for i :=10; i < 25; i++{

	//ss += strconv.Itoa(i)+","
	//log.Printf("====== str:%s ====", ss)
	//}
	//uuid, err := utils.GenUUID()
	//log.Printf("====== uuid:%s ====", uuid)
	var ss string
	bytes := [4]byte{'a', 'b', 'c', 'd'}
	str1 := convert(bytes[:])
	str2 := B2S(bytes[:])
	str := "sdkngklkkklkl"
	buf := S2B(&str)
	str3 := B2S(buf)
	out := rand_test()

	var a int64 = 1
	var b int = 5
	a -= int64(b)
	if a <= 0 {
		fmt.Println("xxxxxxx", int64(b)-a, int64(b)+a)
	}
	fmt.Printf("********* str1:%s *** str2:%s **** str3:%s ***** outchannel:%d ****\n", str1, str2, str3, <-out)
	fmt.Printf("----- buf:%v ------\n", buf)

	nums := []int{2, 3, 4, 5, 6, 7, 8, 9}
	for i := range nums[3:] {
		cur := i + 3
		fmt.Printf("^^^^^^^^ the %v-th value is %v \n", cur, nums[cur])
	}
	ss = "5,6,7,"
	ll := strings.Split(ss, ",")
	logger.Debug("=====xxx test log == before str:%s == end string:%v", ss, ll)

	j1, err := strconv.Atoi(ll[0])
	j2, err := strconv.Atoi(ll[1])
	j3, err := strconv.Atoi(ll[2])
	logger.Debug("= j1:%d == j2:%d == j3:%d ==", j1, j2, j3)
	intList := []int{2, 4, 3, 5, 7, 6, 9, 8, 1, 0}
	sort.Ints(intList)
	fmt.Printf("### %v ###  %v  ### %v  ### %v ### %v ### %v ####\n", intList[9:], intList[:], intList[:3], intList[3:], intList[3:len(intList)-1], intList[10:])
	fmt.Printf("%v %d %d\n", intList, intList[0], intList[len(intList)-1])
	return
	db, err := sql.Open("mysql", "root:root@tcp(127.0.0.1:3306)/jls?charset=utf8")

	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	insert(db)
	count := 1000 / 10
	logger.Debug("xxxx count:", count)
}
