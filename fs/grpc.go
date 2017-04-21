package cfs

import (
	"fmt"
	"github.com/bwits/containerfs/logger"
	"google.golang.org/grpc"
	"time"
)

func DialMeta() (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error

	conn, err = grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
	fmt.Printf("DialMeta 1: addr:%v\n", MetaNodeAddr)
	logger.Error("DialMeta 1: addr:%v", MetaNodeAddr)

	fmt.Printf("DialMeta 1: conn:%v\n", conn)
	logger.Error("DialMeta 1: conn:%v", conn)

	fmt.Printf("DialMeta 1: err:%v\n", err)
	logger.Error("DialMeta 1: err:%v", err)

	if err != nil {
		time.Sleep(300 * time.Millisecond)
		fmt.Printf("DialMeta 2: addr:%v\n", MetaNodeAddr)
		logger.Error("DialMeta 1: addr:%v", MetaNodeAddr)

		conn, err = grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			fmt.Printf("DialMeta 3: addr:%v\n", MetaNodeAddr)
			logger.Error("DialMeta 1: addr:%v", MetaNodeAddr)
			conn, err = grpc.Dial(MetaNodeAddr, grpc.WithInsecure())
		}
	}
	return conn, err
}

func DialData(host string) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error
	conn, err = grpc.Dial(host, grpc.WithInsecure())
	if err != nil {
		time.Sleep(1000 * time.Millisecond)
		conn, err = grpc.Dial(host, grpc.WithInsecure())
		if err != nil {
			time.Sleep(1000 * time.Millisecond)
			conn, err = grpc.Dial(host, grpc.WithInsecure())
		}
	}
	return conn, err
}

func DialVolmgr(host string) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error
	conn, err = grpc.Dial(host, grpc.WithInsecure())
	if err != nil {
		time.Sleep(300 * time.Millisecond)
		conn, err = grpc.Dial(host, grpc.WithInsecure())
		if err != nil {
			time.Sleep(300 * time.Millisecond)
			conn, err = grpc.Dial(host, grpc.WithInsecure())
		}
	}
	return conn, err
}
