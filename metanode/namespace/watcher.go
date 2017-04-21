package namespace

import (
	"bytes"
	"encoding/json"
	"github.com/coreos/etcd/clientv3"
	mp "github.com/bwits/containerfs/proto/mp"
	vp "github.com/bwits/containerfs/proto/vp"
	"github.com/bwits/containerfs/utils"
	"golang.org/x/net/context"

	"fmt"
	"strconv"
)

var EtcdClient utils.EtcdV3
var IsWatcher bool

func (ns *nameSpace) MakeEtcdKeyString(k string, types string, volID string) (key string) {
	var buf bytes.Buffer
	buf.WriteString("/ContainerFS/")
	buf.WriteString(types)
	buf.WriteString("/")
	buf.WriteString(volID)
	buf.WriteString("/")
	buf.WriteString(k)
	key = buf.String()
	return key
}

func (ns *nameSpace) MakeEtcdWatchPreKey(types string, volID string) (key string) {
	var buf bytes.Buffer
	buf.WriteString("/ContainerFS/")
	buf.WriteString(types)
	buf.WriteString("/")
	buf.WriteString(volID)
	buf.WriteString("/")
	key = buf.String()
	return key
}

func (ns *nameSpace) InodeEtcdSet(k string, volID string, v *mp.InodeInfo) {

	var key string
	key = ns.MakeEtcdKeyString(k, "InodeDB", volID)
	val, _ := json.Marshal(v)
	EtcdClient.Set(key, string(val))

}

func (ns *nameSpace) InodeEtcdDelete(k string, volID string) {

	var key string
	key = ns.MakeEtcdKeyString(k, "InodeDB", volID)
	EtcdClient.DoDelete(key)

}

func (ns *nameSpace) CreateInodeDBEtcdWatcher(volID string) {

	var watcher clientv3.WatchChan
	ctx := context.Background()
	watchKey := ns.MakeEtcdWatchPreKey("InodeDB", volID)
	watcher = EtcdClient.Client.Watch(ctx, watchKey, clientv3.WithPrefix())
	for wRes := range watcher {
		if wRes.Err() != nil {

		}
		if IsWatcher {
			for _, ev := range wRes.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					{
						inodeKey := ev.Kv.Key[len(watchKey):]
						inodeInfo := mp.InodeInfo{}
						err := json.Unmarshal([]byte(ev.Kv.Value), &inodeInfo)
						if err != nil {
							fmt.Println("Unmarshal faild")
							continue
						}
						ns.InodeDBSet(string(inodeKey), &inodeInfo)
						ns.InodeDBSet(strconv.FormatInt(inodeInfo.ParentInodeID, 10)+"-"+inodeInfo.Name, &inodeInfo)
					}
				case clientv3.EventTypeDelete:
					{
						inodeKey := ev.Kv.Key[len(watchKey):]
						ok, inodeInfo := ns.InodeDBGet(string(inodeKey))
						if !ok {
							continue
						}
						ns.InodeDBDelete(string(inodeKey))
						ns.InodeDBDelete(strconv.FormatInt(inodeInfo.ParentInodeID, 10) + "-" + inodeInfo.Name)
					}
				}
			}
		}
	}
}

func (ns *nameSpace) BlockGroupEtcdSet(k int32, volID string, v *vp.BlockGroup) {

	var key string
	key = ns.MakeEtcdKeyString(strconv.FormatInt(int64(k), 10), "BGDB", volID)
	val, _ := json.Marshal(v)
	EtcdClient.Set(key, string(val))

}

func (ns *nameSpace) BlockGroupEtcdDelete(k int32, volID string) {

	var key string
	key = ns.MakeEtcdKeyString(strconv.FormatInt(int64(k), 10), "BGDB", volID)
	EtcdClient.DoDelete(key)

}

func (ns *nameSpace) CreateBGDBEtcdWatcher(volID string) {

	var watcher clientv3.WatchChan
	ctx := context.Background()
	watchKey := ns.MakeEtcdWatchPreKey("BGDB", volID)
	watcher = EtcdClient.Client.Watch(ctx, watchKey, clientv3.WithPrefix())
	for wRes := range watcher {
		if wRes.Err() != nil {
		}
		if IsWatcher {
			for _, ev := range wRes.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					{
						bgKey := ev.Kv.Key[len(watchKey):]
						bgInfo := vp.BlockGroup{}
						err := json.Unmarshal([]byte(ev.Kv.Value), &bgInfo)
						if err != nil {
							fmt.Println("Unmarshal faild")
						}

						bgID, _ := strconv.ParseInt(string(bgKey), 10, 64)
						ns.BlockGroupDBSet(int32(bgID), &bgInfo)
					}
				case clientv3.EventTypeDelete:
					{
						bgKey := ev.Kv.Key[len(watchKey):]
						bgID, _ := strconv.ParseInt(string(bgKey), 10, 64)
						ns.BlockGroupDBDelete(int32(bgID))
					}
				}
			}
		}
	}
}

func (ns *nameSpace) ChunkEtcdSet(k int64, volID string, v *mp.ChunkInfo) {

	var key string
	key = ns.MakeEtcdKeyString(strconv.FormatInt(k, 10), "ChunkDB", volID)
	val, _ := json.Marshal(v)
	EtcdClient.Set(key, string(val))

}

func (ns *nameSpace) ChunkEtcdDelete(k int64, volID string) {

	var key string
	key = ns.MakeEtcdKeyString(strconv.FormatInt(k, 10), "ChunkDB", volID)
	EtcdClient.DoDelete(key)

}

func (ns *nameSpace) CreateChunkDBEtcdWatcher(volID string) {

	var watcher clientv3.WatchChan
	ctx := context.Background()
	watchKey := ns.MakeEtcdWatchPreKey("ChunkDB", volID)
	watcher = EtcdClient.Client.Watch(ctx, watchKey, clientv3.WithPrefix())
	for wRes := range watcher {
		if wRes.Err() != nil {
		}
		if IsWatcher {
			for _, ev := range wRes.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					{
						chunkKey := ev.Kv.Key[len(watchKey):]
						chunkInfo := mp.ChunkInfo{}

						err := json.Unmarshal([]byte(ev.Kv.Value), &chunkInfo)
						if err != nil {
							fmt.Println("Unmarshal faild")
						}

						chunkID, _ := strconv.ParseInt(string(chunkKey), 10, 64)
						ns.ChunkDBSet(chunkID, &chunkInfo)
					}
				case clientv3.EventTypeDelete:
					{
						chunkKey := ev.Kv.Key[len(watchKey):]
						chunkID, _ := strconv.ParseInt(string(chunkKey), 10, 64)
						ns.ChunkDBDelete(chunkID)
					}
				}
			}
		}
	}
}

func (ns *nameSpace) InodeBaseIDEtcdSet(v string, volID string) {

	var key string
	key = ns.MakeEtcdKeyString("InodeBaseIDKey", "InodeBaseID", volID)
	EtcdClient.Set(key, v)

}

func (ns *nameSpace) CreateInodeBaseIDEtcdWatcher(volID string) {

	var watcher clientv3.WatchChan
	ctx := context.Background()
	watchKey := ns.MakeEtcdWatchPreKey("InodeBaseID", volID) + "InodeBaseIDKey"
	watcher = EtcdClient.Client.Watch(ctx, watchKey, clientv3.WithPrefix())
	for wRes := range watcher {
		if wRes.Err() != nil {
		}
		if IsWatcher {
			for _, ev := range wRes.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					{
						baseInodeID, _ := strconv.ParseInt(string(ev.Kv.Value), 10, 64)
						ns.BaseInodeID = utils.New(baseInodeID+1, 1)
					}
				}
			}
		}
	}
}

func (ns *nameSpace) ChunkBaseIDEtcdSet(v string, volID string) {

	var key string
	key = ns.MakeEtcdKeyString("ChunkBaseIDKey", "ChunkBaseID", volID)
	EtcdClient.Set(key, v)

}

func (ns *nameSpace) CreateChunkBaseIDEtcdWatcher(volID string) {

	var watcher clientv3.WatchChan
	ctx := context.Background()
	watchKey := ns.MakeEtcdWatchPreKey("ChunkBaseID", volID) + "ChunkBaseIDKey"
	watcher = EtcdClient.Client.Watch(ctx, watchKey, clientv3.WithPrefix())
	for wRes := range watcher {
		if wRes.Err() != nil {
		}
		if IsWatcher {
			for _, ev := range wRes.Events {
				switch ev.Type {
				case clientv3.EventTypePut:
					{
						baseChunkID, _ := strconv.ParseInt(string(ev.Kv.Value), 10, 64)
						ns.BaseChunkID = utils.New(baseChunkID+1, 1)
					}
				}
			}
		}
	}
}
