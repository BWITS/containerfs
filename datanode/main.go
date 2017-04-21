package main

import (
	//"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	//"io/ioutil"
	"bufio"
	"github.com/bwits/containerfs/logger"
	dp "github.com/bwits/containerfs/proto/dp"
	vp "github.com/bwits/containerfs/proto/vp"
	"github.com/bwits/containerfs/utils"
	"github.com/lxmgo/config"
	"net"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type DataNodeServer struct {
	Mutex sync.Mutex
}

type addr struct {
	Ipnr  net.IP
	IpInt int32
	IpStr string
	Port  int32
	Path  string
	Flag  string
	Log   string

	VolMgrIp   string
	VolMgrPort string
}

var DataNodeServerAddr addr

func startDataService() {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", DataNodeServerAddr.Port))
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", DataNodeServerAddr.Port))
	}
	s := grpc.NewServer()
	dp.RegisterDataNodeServer(s, &DataNodeServer{})
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

func registryToVolMgr() {
	conn, err := grpc.Dial(DataNodeServerAddr.VolMgrIp+":"+DataNodeServerAddr.VolMgrPort, grpc.WithInsecure())
	if err != nil {
		logger.Debug("data node statup failed : Dial to volmgr failed !")
		os.Exit(1)
	}
	defer conn.Close()
	c := vp.NewVolMgrClient(conn)

	var datanodeRegistryReq vp.DatanodeRegistryReq
	datanodeRegistryReq.Ip = DataNodeServerAddr.IpInt
	datanodeRegistryReq.Port = DataNodeServerAddr.Port
	diskInfo := utils.DiskUsage(DataNodeServerAddr.Path)
	capacity := int32(float64(diskInfo.All) / float64(1024*1024*1024))
	datanodeRegistryReq.Capacity = capacity
	datanodeRegistryReq.MountPoint = DataNodeServerAddr.Path

	_, err = os.Stat(datanodeRegistryReq.MountPoint)
	if err != nil {
		logger.Error("data node statup failed : DataNodeServerAddr.Path not exist !")
		os.Exit(1)
	}

	pDatanodeRegistryAck, _ := c.DatanodeRegistry(context.Background(), &datanodeRegistryReq)
	if pDatanodeRegistryAck.Ret == 0 {
		logger.Debug("registry success!")
		os.Create(DataNodeServerAddr.Flag)
		for i := pDatanodeRegistryAck.StartBlockID; i <= pDatanodeRegistryAck.EndBlockID; i++ {
			os.MkdirAll(DataNodeServerAddr.Path+"/block-"+strconv.Itoa(int(i)), 0777)
		}
	} else {
		logger.Debug("data node statup failed : registry to volmgr failed !")
		os.Exit(1)
	}

	return

}

func heartbeatToVolMgr() {

	conn, err := grpc.Dial(DataNodeServerAddr.VolMgrIp+":"+DataNodeServerAddr.VolMgrPort, grpc.WithInsecure())
	if err != nil {
		logger.Debug("HearBeat failed : Dial to volmgr failed !")
	}
	defer conn.Close()
	c := vp.NewVolMgrClient(conn)

	diskInfo := utils.DiskUsage(DataNodeServerAddr.Path)
	free := int32(float64(diskInfo.Free) / float64(1024*1024*1024))
	used := int32(float64(diskInfo.Used) / float64(1024*1024*1024))

	var datanodeHeartbeatReq vp.DatanodeHeartbeatReq
	datanodeHeartbeatReq.Ip = DataNodeServerAddr.IpInt
	datanodeHeartbeatReq.Port = DataNodeServerAddr.Port
	datanodeHeartbeatReq.Free = free
	datanodeHeartbeatReq.Used = used
	datanodeHeartbeatReq.Status = 0

	c.DatanodeHeartbeat(context.Background(), &datanodeHeartbeatReq)
}

/*
rpc GetChunks(GetChunksReq) returns (GetChunksAck){};
*/
func (s *DataNodeServer) DatanodeHealthCheck(ctx context.Context, in *dp.DatanodeHealthCheckReq) (*dp.DatanodeHealthCheckAck, error) {
	ack := dp.DatanodeHealthCheckAck{}
	ack.Ret = 1
	return &ack,nil
}

func (s *DataNodeServer) WriteChunk(ctx context.Context, in *dp.WriteChunkReq) (*dp.WriteChunkAck, error) {
	var f *os.File
	var err error

	ack := dp.WriteChunkAck{}
	chunkID := in.ChunkID
	blockID := in.BlockID

	chunkFileName := DataNodeServerAddr.Path + "/block-" + strconv.Itoa(int(blockID)) + "/chunk-" + strconv.Itoa(int(chunkID))

	f, err = os.OpenFile(chunkFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0660)
	defer f.Close()
	if err != nil {
		ack.Ret = -1
		return &ack, nil
	}
	w := bufio.NewWriter(f)
	w.Write(in.Databuf)
	w.Flush()

	ack.Ret = 0
	return &ack, nil
}

/*rpc WriteChunkStream(stream WriteChunkReq) returns (WriteChunkAck){}; */
/*
func (s *DataNodeServer) WriteChunkStream(stream dp.DataNode_WriteChunkStreamServer) error {

	var f *os.File
	var err error
	//fmt.Println("writechunking in datanode ...")
	ack := dp.WriteChunkAck{}
	if err != nil {
		ack.Ret = -1
		return stream.SendAndClose(&ack)
	}
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			ack.Ret = 0
			return stream.SendAndClose(&ack)
		}
		if err != nil {
			ack.Ret = 1
			return stream.SendAndClose(&ack)
		}
		chunkID := in.ChunkID
		blockID := in.BlockID
		chunkFileName := DataNodeServerAddr.Path + "/block-" + strconv.Itoa(int(blockID)) + "/chunk-" + strconv.Itoa(int(chunkID))
		f, err = os.OpenFile(chunkFileName, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		f.WriteString(in.Databuf)
		f.Close()
	}
}
*/
/*
func (s *DataNodeServer) ReadChunk(ctx context.Context, in *dp.ReadChunkReq) (*dp.ReadChunkAck, error) {
	ack := dp.ReadChunkAck{}
	chunkID := in.ChunkID
	blockID := in.BlockID
	readsize := in.Readlen
	offset := in.Offset

	chunkFileName := DataNodeServerAddr.Path + "/block-" + strconv.Itoa(int(blockID)) + "/chunk-" + strconv.Itoa(int(chunkID))
	f, err := os.Open(chunkFileName)
	defer f.Close()
	if err != nil {
		ack.Ret = -1
		return &ack, nil
	}
	_, err = f.Seek(offset, 0)
	if err != nil {
		ack.Ret = -1
		return &ack, nil
	}

	buf := make([]byte, readsize)
	bfRd := bufio.NewReader(f)
	for {
		n, err := bfRd.Read(buf)
		if err != nil {
			ack.Ret = -1
			return &ack, nil
		}
		ack.Databuf = utils.B2S(buf)
		ack.Ret = 1
		ack.Readsize = int64(n)
		return &ack, nil
	}
}
*/
func (s *DataNodeServer) StreamReadChunk(in *dp.StreamReadChunkReq, stream dp.DataNode_StreamReadChunkServer) error {
	chunkID := in.ChunkID
	blockID := in.BlockID
	offset := in.Offset
	readsize := in.Readsize

	chunkFileName := DataNodeServerAddr.Path + "/block-" + strconv.Itoa(int(blockID)) + "/chunk-" + strconv.Itoa(int(chunkID))
	f, err := os.Open(chunkFileName)
	defer f.Close()
	if err != nil {
		return err
	}
	_, err = f.Seek(offset, 0)
	if err != nil {
		return err
	}

	var ack dp.StreamReadChunkAck
	totalsize := readsize
	buf := make([]byte, 1024*1024)
	bfRd := bufio.NewReader(f)
	for {
		n, err := bfRd.Read(buf)
		if err != nil {
			return err
		}

		totalsize -= int64(n)
		if totalsize <= 0 {
			var m int64
			m = int64(n) + totalsize
			ack.Databuf = buf[:m]
			if err := stream.Send(&ack); err != nil {
				return err
			}
			break
		}
		ack.Databuf = buf[:n]
		if err := stream.Send(&ack); err != nil {
			return err
		}
	}

	return nil

}

/*
rpc DeleteChunks(eleteChunksReq) returns (eleteChunksAck){};
*/
func (s *DataNodeServer) DeleteChunk(ctx context.Context, in *dp.DeleteChunkReq) (*dp.DeleteChunkAck, error) {
	var err error

	ack := dp.DeleteChunkAck{}
	chunkID := in.ChunkID
	blockID := in.BlockID

	chunkFileName := DataNodeServerAddr.Path + "/block-" + strconv.Itoa(int(blockID)) + "/chunk-" + strconv.Itoa(int(chunkID))

	err = os.Remove(chunkFileName)
	if err != nil {
		ack.Ret = 0
	} else {
		ack.Ret = 0
	}
	ack.Ret = 0
	return &ack, nil
}

func init() {
	c, err := config.NewConfig(os.Args[1])
	if err != nil {
		fmt.Println("NewConfig err")
		os.Exit(1)
	}

	DataNodeServerAddr.IpStr = c.String("host")
	ipnr := net.ParseIP(DataNodeServerAddr.IpStr)
	DataNodeServerAddr.Ipnr = ipnr
	ipint := utils.Inet_aton(ipnr)
	DataNodeServerAddr.IpInt = ipint
	port, _ := c.Int("port")
	DataNodeServerAddr.Port = int32(port)
	DataNodeServerAddr.Path = c.String("path")
	DataNodeServerAddr.Log = c.String("log")
	DataNodeServerAddr.Flag = DataNodeServerAddr.Path + "/.registryflag"
	DataNodeServerAddr.VolMgrIp = c.String("volmgr::host")
	DataNodeServerAddr.VolMgrPort = c.String("volmgr::port")

	logger.SetConsole(true)
	logger.SetRollingFile(DataNodeServerAddr.Log, "datanode.log", 10, 100, logger.MB) //each 100M rolling

	switch level := c.String("loglevel"); level {
	case "error":
		logger.SetLevel(logger.ERROR)
	case "debug":
		logger.SetLevel(logger.DEBUG)
	case "info":
		logger.SetLevel(logger.INFO)
	default:
		logger.SetLevel(logger.ERROR)
	}

	if ok, _ := utils.LocalPathExists(DataNodeServerAddr.Flag); !ok {
		logger.Debug("Start registy to volmgr ...")
		registryToVolMgr()
		logger.Debug("registy to volmgr sucess")
	} else {
		logger.Debug("already registied")
	}

}

func main() {

	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	ticker := time.NewTicker(time.Second * 60)
	go func() {
		for _ = range ticker.C {
			heartbeatToVolMgr()
		}
	}()
	startDataService()
}
