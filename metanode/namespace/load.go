package namespace

import (
	"encoding/json"
	"github.com/bwits/containerfs/logger"
	mp "github.com/bwits/containerfs/proto/mp"
	vp "github.com/bwits/containerfs/proto/vp"
	"github.com/bwits/containerfs/utils"
	"strconv"
	"time"
)

type VolMetaWatcherVersion struct {
	ChunkBaseIDVersion int64
	InodeBaseIDVersion int64
	ChunkDBVersion     int64
	BGDBVersion        int64
	InodeDBVersion     int64
}

func LoadVolMeta(volID string) {

	logger.Error("LoadVolMeta volID %v\n", volID)

	ret, nameSpace := GetNameSpace(volID)
	if ret != 0 {
		return
	}

	key := nameSpace.MakeEtcdWatchPreKey("ChunkBaseID", volID)
	key += "ChunkBaseIDKey"
	time.Sleep(time.Millisecond * 100)
	resp, err := EtcdClient.Get(key, false)
	if err != nil {
		logger.Error("err:%v\n", err)
		return
	}

	baseChunkID, _ := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	nameSpace.BaseChunkID = utils.New(baseChunkID+1, 1)

	key = nameSpace.MakeEtcdWatchPreKey("InodeBaseID", volID)
	key += "InodeBaseIDKey"
	resp, err = EtcdClient.Get(key, false)
	if err != nil {
		logger.Error("err:%v\n", err)
		return
	}
	baseInodeID, _ := strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
	nameSpace.BaseInodeID = utils.New(baseInodeID+1, 1)

	preKey := nameSpace.MakeEtcdWatchPreKey("ChunkDB", volID)
	resp, err = EtcdClient.Get(preKey, true)
	if err != nil {
		logger.Error("err:%v\n", err)
		return
	}

	for _, ev := range resp.Kvs {
		chunkKey := ev.Key[len(preKey):]
		chunkInfo := mp.ChunkInfo{}

		err := json.Unmarshal([]byte(ev.Value), &chunkInfo)
		if err != nil {
			logger.Error("Unmarshal faild")
		}

		chunkID, _ := strconv.ParseInt(string(chunkKey), 10, 64)
		nameSpace.ChunkDBSet(chunkID, &chunkInfo)
	}

	preKey = nameSpace.MakeEtcdWatchPreKey("BGDB", volID)
	resp, err = EtcdClient.Get(preKey, true)
	if err != nil {
		logger.Error("err:%v\n", err)
		return
	}
	for _, ev := range resp.Kvs {
		bgKey := ev.Key[len(preKey):]
		bgID, _ := strconv.Atoi(string(bgKey))

		bgInfo := vp.BlockGroup{}
		err = json.Unmarshal([]byte(ev.Value), &bgInfo)
		if err != nil {
			logger.Error("Unmarshal faild")
		}
		nameSpace.BlockGroupDBSet(int32(bgID), &bgInfo)
	}

	preKey = nameSpace.MakeEtcdWatchPreKey("InodeDB", volID)
	resp, err = EtcdClient.Get(preKey, true)
	if err != nil {
		logger.Error("err:%v\n", err)
		return
	}

	for _, ev := range resp.Kvs {
		inodeKey := ev.Key[len(preKey):]
		inodeID, _ := strconv.Atoi(string(inodeKey))

		inodeInfo := mp.InodeInfo{}
		err = json.Unmarshal([]byte(ev.Value), &inodeInfo)
		if err != nil {
			logger.Error("Unmarshal faild")
		}
		nameSpace.InodeDBSet(strconv.FormatInt(int64(inodeID), 10), &inodeInfo)
		nameSpace.InodeDBSet(strconv.FormatInt(inodeInfo.ParentInodeID, 10)+"-"+inodeInfo.Name, &inodeInfo)

	}

	time.Sleep(time.Millisecond * 500)
	go WatchVolMeta(volID)
}

func WatchVolMeta(volID string) {
	ret, nameSpace := GetNameSpace(volID)
	if ret != 0 {
		return
	}
	go nameSpace.CreateChunkBaseIDEtcdWatcher(volID)
	go nameSpace.CreateInodeBaseIDEtcdWatcher(volID)
	go nameSpace.CreateChunkDBEtcdWatcher(volID)
	go nameSpace.CreateBGDBEtcdWatcher(volID)
	go nameSpace.CreateInodeDBEtcdWatcher(volID)
}
