package main

import (
	"fmt"
	"github.com/bwits/containerfs/logger"

	ns "github.com/bwits/containerfs/metanode/namespace"
	mRaft "github.com/bwits/containerfs/metanode/raft"
	mp "github.com/bwits/containerfs/proto/mp"

	"github.com/lxmgo/config"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type addr struct {
	host   string
	port   int
	peer   []string
	domain string
	log    string
}

var MetaNodeServerAddr addr

type MetaNodeServer struct {
	Mutex sync.Mutex
}

/*
rpc CreateNameSpace(CreateNameSpaceReq) returns (CreateNameSpaceAck){};
*/
func (s *MetaNodeServer) CreateNameSpace(ctx context.Context, in *mp.CreateNameSpaceReq) (*mp.CreateNameSpaceAck, error) {
	ack := mp.CreateNameSpaceAck{}
	ns.Wg.Add(1)
	ack.Ret = ns.CreateNameSpace(in.VolID, false)

	if mRaft.RaftInfo.R.IsLeader() {
		// send to follower metadatas to registry a new map

		for _, addr := range MetaNodeServerAddr.peer {
			conn2, err2 := grpc.Dial(addr, grpc.WithInsecure())
			if err2 != nil {
				logger.Error("Leader told Follower to create NameSpace Failed ...")
				continue
			}
			defer conn2.Close()
			mc := mp.NewMetaNodeClient(conn2)
			pmCreateNameSpaceReq := &mp.CreateNameSpaceReq{
				VolID: in.VolID,
			}
			pmCreateNameSpaceAck, ret := mc.CreateNameSpace(context.Background(), pmCreateNameSpaceReq)
			if ret != nil {
				logger.Error("Leader told Follower to create NameSpace Failed ...")
				continue
			}
			if pmCreateNameSpaceAck.Ret != 0 {
				logger.Error("Leader told Follower to create NameSpace Failed ...")
				continue
			}
		}

	}
	return &ack, nil
}

/*
rpc GetFSInfo(GetFSInfoReq) returns (GetFSInfoAck){};
*/
func (s *MetaNodeServer) GetFSInfo(ctx context.Context, in *mp.GetFSInfoReq) (*mp.GetFSInfoAck, error) {
	ack := mp.GetFSInfoAck{}

	ret, nameSpace := ns.GetNameSpace(in.VolID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}

	ack = nameSpace.GetFSInfo(in.VolID)
	return &ack, nil
}

/*
rpc CreateDir(CreateDirReq) returns (CreateDirAck){};
*/
func (s *MetaNodeServer) CreateDir(ctx context.Context, in *mp.CreateDirReq) (*mp.CreateDirAck, error) {
	ack := mp.CreateDirAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.CreateDir(fullPathName)
	return &ack, nil
}

/*
rpc StatDir(StatDirReq) returns (StatDirAck){};
*/
func (s *MetaNodeServer) Stat(ctx context.Context, in *mp.StatReq) (*mp.StatAck, error) {
	ack := mp.StatAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.InodeInfo, ack.Ret = nameSpace.Stat(fullPathName)
	return &ack, nil
}

/*
rpc ListDir(StatDirReq) returns (ListDirAck){};
*/
func (s *MetaNodeServer) List(ctx context.Context, in *mp.ListReq) (*mp.ListAck, error) {
	ack := mp.ListAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.InodeInfos, ack.Ret = nameSpace.List(fullPathName)
	return &ack, nil
}

/*
rpc DeleteDir(DeleteDirReq) returns (DeleteDirAck){};
*/
func (s *MetaNodeServer) DeleteDir(ctx context.Context, in *mp.DeleteDirReq) (*mp.DeleteDirAck, error) {

	ack := mp.DeleteDirAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.DeleteDir(fullPathName)
	return &ack, nil

}

/*
rpc Rename(RenameReq) returns (RenameAck){};
*/
func (s *MetaNodeServer) Rename(ctx context.Context, in *mp.RenameReq) (*mp.RenameAck, error) {
	ack := mp.RenameAck{}
	fullPathName1 := in.FullPathName1
	fullPathName2 := in.FullPathName2
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.Rename(fullPathName1, fullPathName2)
	return &ack, nil

}

/*
rpc CreateFile(CreateFileReq) returns (CreateFileAck){};
*/
func (s *MetaNodeServer) CreateFile(ctx context.Context, in *mp.CreateFileReq) (*mp.CreateFileAck, error) {
	ack := mp.CreateFileAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.CreateFile(fullPathName)
	return &ack, nil
}

/*
rpc DeleteFile(DeleteFileReq) returns (DeleteFileAck){};
*/
func (s *MetaNodeServer) DeleteFile(ctx context.Context, in *mp.DeleteFileReq) (*mp.DeleteFileAck, error) {

	ack := mp.DeleteFileAck{}
	fullPathName := in.FullPathName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.DeleteFile(fullPathName)
	return &ack, nil

}

/*
rpc AllocateChunk(AllocateChunkReq) returns (AllocateChunkAck){};
*/
func (s *MetaNodeServer) AllocateChunk(ctx context.Context, in *mp.AllocateChunkReq) (*mp.AllocateChunkAck, error) {
	ack := mp.AllocateChunkAck{}
	fileName := in.FileName
	volID := in.VolID

	ack.SequenceID = in.SequenceID

	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ok, chunkInfo := nameSpace.AllocateChunk(fileName)
	if ok != 0 {
		ack.Ret = 1
		return &ack, nil
	}

	ok1, blockGroup := nameSpace.BlockGroupDBGet(chunkInfo.BlockGroupID)
	if !ok1 {
		ack.Ret = 1
		return &ack, nil
	}

	var tmpChunkInfo mp.ChunkInfoWithBG
	tmpChunkInfo.ChunkID = chunkInfo.ChunkID
	tmpChunkInfo.ChunkSize = chunkInfo.ChunkSize
	tmpChunkInfo.BlockGroup = nameSpace.BlockGroupVp2Mp(blockGroup)

	ack.ChunkInfo = &tmpChunkInfo
	return &ack, nil
}

/*
rpc GetChunks(GetChunksReq) returns (GetChunksAck){};
*/
func (s *MetaNodeServer) GetFileChunks(ctx context.Context, in *mp.GetFileChunksReq) (*mp.GetFileChunksAck, error) {
	ack := mp.GetFileChunksAck{}
	fileName := in.FileName
	volID := in.VolID
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ok, chunkInfos := nameSpace.GetFileChunks(fileName)
	if ok != 0 {
		ack.Ret = ok
		return &ack, nil
	}

	for _, v := range chunkInfos {
		var chunkInfoWithBG mp.ChunkInfoWithBG
		chunkInfoWithBG.ChunkID = v.ChunkID
		chunkInfoWithBG.ChunkSize = v.ChunkSize

		ok1, blockGroup := nameSpace.BlockGroupDBGet(v.BlockGroupID)
		if !ok1 {
			continue
		}
		chunkInfoWithBG.BlockGroup = nameSpace.BlockGroupVp2Mp(blockGroup)

		ack.ChunkInfos = append(ack.ChunkInfos, &chunkInfoWithBG)

	}
	ack.Ret = 0

	return &ack, nil
}

/*
rpc GetChunks(GetChunksReq) returns (GetChunksAck){};
*/
func (s *MetaNodeServer) SyncChunk(ctx context.Context, in *mp.SyncChunkReq) (*mp.SyncChunkAck, error) {
	ack := mp.SyncChunkAck{}
	fileName := in.FileName
	volID := in.VolID
	chunkinfo := in.ChunkInfo
	ret, nameSpace := ns.GetNameSpace(volID)
	if ret != 0 {
		ack.Ret = ret
		return &ack, nil
	}
	ack.Ret = nameSpace.SyncChunk(fileName, chunkinfo)
	return &ack, nil
}

func startMetaDataService() {

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", MetaNodeServerAddr.port))
	if err != nil {
		panic(fmt.Sprintf("Failed to listen on:%v", MetaNodeServerAddr.port))
	}
	s := grpc.NewServer()
	mp.RegisterMetaNodeServer(s, &MetaNodeServer{})
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		panic("Failed to serve")
	}
}

func loadMetaData() {
	ns.CreateGNameSpace()
	ret, vols := ns.GetVolList()
	if ret != 0 {
		logger.Error("loadMetaData,GetVolList failed,ret:%v", ret)
		return
	}
	for _, v := range vols {
		ns.Wg.Add(1)
		go ns.CreateNameSpace(v, true)
	}
	ns.Wg.Wait()
}

func watchMetaData() {
	ret, vols := ns.GetVolList()
	if ret != 0 {
		logger.Error("watchMetaData,GetVolList failed,ret:%v", ret)
		return
	}
	for _, v := range vols {
		ns.WatchVolMeta(v)
	}
}

func init() {
	//ns.CreateGNameSpace()

	c, err := config.NewConfig(os.Args[1])
	if err != nil {
		fmt.Println("NewConfig err")
		os.Exit(1)
	}

	MetaNodeServerAddr.host = c.String("host")
	port, _ := c.Int("port")
	MetaNodeServerAddr.port = port
	MetaNodeServerAddr.peer = c.Strings("peer")
	MetaNodeServerAddr.log = c.String("log")

	ns.VolMgrAddress = c.String("volmgr::host")

	mRaft.RaftInfo.Me = c.String("raft::me")
	s := strings.Split(mRaft.RaftInfo.Me, ":")
	mRaft.RaftInfo.MePort, _ = strconv.Atoi(s[1])
	mRaft.RaftInfo.Peer = c.Strings("raft::peer")

	logger.SetConsole(true)
	logger.SetRollingFile(MetaNodeServerAddr.log, "metanode.log", 10, 100, logger.MB) //each 100M rolling
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

	//endPoints := strings.Split(c.String("etcd::endpoints"), ",")
	endPoints := c.Strings("etcd::endpoints")
	err = ns.EtcdClient.InitEtcd(endPoints)
	if err != nil {
		fmt.Println("connect etcd failed")
		os.Exit(0)
	}

	mRaft.StartRaftService()
	loadMetaData()
	watchMetaData()

}

func setMetaLeader() {
	fmt.Println("set leader...")
	ns.EtcdClient.Set("/ContainerFS/MetaLeader", MetaNodeServerAddr.host+":"+strconv.Itoa(MetaNodeServerAddr.port))
}

func main() {

	//for multi-cpu scheduling
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)

	ticker1 := time.NewTicker(time.Millisecond * 5)
	var myRole bool = true
	var startUp bool = true

	go func() {
		for _ = range ticker1.C {
			if mRaft.RaftInfo.R.IsLeader() {
				if startUp {
					setMetaLeader()
				}
				startUp = false
				if myRole != mRaft.RaftInfo.R.IsLeader() {
					// sleep 200ms for watch all the last event befor change to unWatcher
					time.Sleep(time.Millisecond * 200)
					ns.IsWatcher = false
					setMetaLeader()
				}
				myRole = true
				ns.IsWatcher = false
			} else {
				myRole = false
				ns.IsWatcher = true
			}
		}
	}()

	startMetaDataService()
}
