package namespace

import (
	"bytes"
	mp "github.com/bwits/containerfs/proto/mp"
	vp "github.com/bwits/containerfs/proto/vp"

	"github.com/bwits/containerfs/logger"
	"github.com/bwits/containerfs/utils"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	Blksize = 10 /*G*/
)

var Wg sync.WaitGroup
var VolMgrAddress string

type nameSpace struct {
	VolID        string
	InodeDB      map[string]*mp.InodeInfo
	Mutex        sync.RWMutex
	BlockGroupDB map[int32]*vp.BlockGroup
	BGMutex      sync.RWMutex
	ChunkDB      map[int64]*mp.ChunkInfo
	CMutex       sync.RWMutex
	BaseInodeID  *utils.AutoInc
	BaseChunkID  *utils.AutoInc
}

var AllNameSpace map[string]*nameSpace
var gMutex sync.RWMutex

func CreateGNameSpace() {
	gMutex.Lock()
	AllNameSpace = make(map[string]*nameSpace)
	gMutex.Unlock()
}
func CreateNameSpace(UUID string, IsLoad bool) int32 {
	nameSpace := nameSpace{}
	nameSpace.VolID = UUID
	nameSpace.InodeDB = make(map[string]*mp.InodeInfo)
	nameSpace.BlockGroupDB = make(map[int32]*vp.BlockGroup)
	nameSpace.ChunkDB = make(map[int64]*mp.ChunkInfo)
	//nameSpace.BaseInodeID = utils.New(inodenum, 1)
	//nameSpace.BaseChunkID = utils.New(chunknum, 1)

	gMutex.Lock()
	AllNameSpace[UUID] = &nameSpace
	gMutex.Unlock()

	if !IsLoad {
		ret, tmpBlockGroups := nameSpace.GetVolInfo(UUID)
		if ret != 0 {
			return ret
		}
		for _, v := range tmpBlockGroups {
			v.FreeCnt = 160
			nameSpace.BlockGroupDBSet(v.BlockGroupID, v)
			nameSpace.BlockGroupEtcdSet(v.BlockGroupID, UUID, v)
		}

		tmpInodeInfo := mp.InodeInfo{
			InodeID: 0, Name: "/",
			AccessTime: time.Now().Unix(),
			ModifiTime: time.Now().Unix(),
			InodeType:  false}
		nameSpace.InodeDBSet("0", &tmpInodeInfo)
		nameSpace.InodeEtcdSet("0", UUID, &tmpInodeInfo)

		tmpChunkInfo := mp.ChunkInfo{}
		nameSpace.ChunkDBSet(0, &tmpChunkInfo)
		nameSpace.ChunkEtcdSet(0, UUID, &tmpChunkInfo)

		nameSpace.InodeBaseIDEtcdSet("0", UUID)
		nameSpace.ChunkBaseIDEtcdSet("0", UUID)

	}

	LoadVolMeta(UUID)
	Wg.Add(-1)
	return 0
}

func GetNameSpace(UUID string) (int32, *nameSpace) {
	gMutex.RLock()
	defer gMutex.RUnlock()
	if v, ok := AllNameSpace[UUID]; ok {
		return 0, v
	} else {
		return -1, nil
	}
}

func (ns *nameSpace) GetFSInfo(volID string) mp.GetFSInfoAck {
	ack := mp.GetFSInfoAck{}
	var totalSpace uint64 = 0
	var freeSpace uint64 = 0
	ns.BGMutex.Lock()
	for _, v := range ns.BlockGroupDB {
		totalSpace = totalSpace + (Blksize * 1073741824)
		freeSpace = freeSpace + 64*1024*1024*uint64(v.FreeCnt)
	}
	ns.BGMutex.Unlock()
	ack.TotalSpace = totalSpace
	ack.FreeSpace = freeSpace
	ack.Ret = 0
	return ack
}

func (ns *nameSpace) GetVolInfo(name string) (int32, []*vp.BlockGroup) {
	conn, err := grpc.Dial(VolMgrAddress, grpc.WithInsecure())
	if err != nil {
		logger.Debug("Dial failed: %v", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetVolInfoReq := &vp.GetVolInfoReq{UUID: name}
	pGetVolInfoAck, _ := vc.GetVolInfo(context.Background(), pGetVolInfoReq)
	if pGetVolInfoAck.Ret != 0 {
		logger.Debug("GetVolInfo failed: %v", pGetVolInfoAck.Ret)
		return pGetVolInfoAck.Ret, nil
	}
	return 0, pGetVolInfoAck.VolInfo.BlockGroups
}

func GetVolList() (int32, []string) {
	conn, err := grpc.Dial(VolMgrAddress, grpc.WithInsecure())
	if err != nil {
		logger.Debug("Dial failed: %v", err)
		return -1, nil
	}
	defer conn.Close()
	vc := vp.NewVolMgrClient(conn)
	pGetVolListReq := &vp.GetVolListReq{}
	pGetVolListAck, _ := vc.GetVolList(context.Background(), pGetVolListReq)
	if pGetVolListAck.Ret != 0 {
		logger.Debug("GetVolList failed: %v", pGetVolListAck.Ret)
		return pGetVolListAck.Ret, nil
	}
	return 0, pGetVolListAck.VolIDs
}

func (ns *nameSpace) CreateDir(path string) int32 {
	var ret int32
	ret = 0
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)
	var pParentInodeInfo *mp.InodeInfo
	var ok bool
	if ok, pParentInodeInfo = ns.InodeDBGet(keys[keysNum-2]); !ok {
		ret = 2 /*ENOENT*/
		return ret
	}
	if pParentInodeInfo.InodeType == true {
		ret = 1 /*EPERM*/
		return ret
	}
	if ok, _ = ns.InodeDBGet(keys[keysNum-1]); ok {
		ret = 17 /*ENOENT*/
		return ret
	}
	/*update inode info*/
	inodeID := ns.AllocateInodeID()
	name := utils.GetSelfName(path)
	tmpInodeInfo := mp.InodeInfo{
		ParentInodeID: pParentInodeInfo.InodeID,
		InodeID:       inodeID,
		Name:          name,
		AccessTime:    time.Now().Unix(),
		ModifiTime:    time.Now().Unix(),
		InodeType:     false}

	tmpKey := strconv.FormatInt(inodeID, 10)
	ns.InodeDBSet(tmpKey, &tmpInodeInfo)
	ns.InodeEtcdSet(tmpKey, ns.VolID, &tmpInodeInfo)

	tmpKey = strconv.FormatInt(pParentInodeInfo.InodeID, 10) + "-" + name
	ns.InodeDBSet(tmpKey, &tmpInodeInfo)
	/*update patent inode info*/
	pParentInodeInfo.ChildrenInodeIDs = append(pParentInodeInfo.ChildrenInodeIDs, inodeID)
	parentName := utils.GetParentName(path)
	if parentName == "/" {
		ns.InodeDBSet("0", pParentInodeInfo)
		ns.InodeEtcdSet("0", ns.VolID, pParentInodeInfo)
	} else {
		tmpKey = strconv.FormatInt(pParentInodeInfo.InodeID, 10)
		ns.InodeDBSet(tmpKey, pParentInodeInfo)
		ns.InodeEtcdSet(tmpKey, ns.VolID, pParentInodeInfo)

		tmpKey = strconv.FormatInt(pParentInodeInfo.ParentInodeID, 10) + "-" + utils.GetParentName(path)
		ns.InodeDBSet(tmpKey, pParentInodeInfo)
	}
	return ret
}

func (ns *nameSpace) Stat(path string) (*mp.InodeInfo, int32) {
	var ret int32
	ret = 0
	var ok bool
	var pInodeInfo *mp.InodeInfo

	if path == "/" {
		_, pInodeInfo = ns.InodeDBGet("0")
		return pInodeInfo, 0
	}
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	if ok, pInodeInfo = ns.InodeDBGet(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return nil, ret
	}
	return pInodeInfo, 0
}

func (ns *nameSpace) List(path string) ([]*mp.InodeInfo, int32) {
	var ret int32
	ret = 0

	var pInodeInfo *mp.InodeInfo
	var ok bool
	if path == "/" {
		_, pInodeInfo = ns.InodeDBGet("0")
	} else {
		keys := ns.GetAllKeyByFullPath(path)
		keysNum := len(keys)

		if ok, pInodeInfo = ns.InodeDBGet(keys[keysNum-1]); !ok {
			ret = 2 /*ENOENT*/
			return nil, ret
		}
	}
	var tmpInodeInfos []*mp.InodeInfo
	if pInodeInfo.InodeType == true {
		tmpInodeInfos = append(tmpInodeInfos, pInodeInfo)
		return tmpInodeInfos, ret
	}

	var pTmpInodeInfo *mp.InodeInfo
	for _, value := range pInodeInfo.ChildrenInodeIDs {
		ok, pTmpInodeInfo = ns.InodeDBGet(strconv.FormatInt(value, 10))
		if !ok {
			continue
		}
		tmpInodeInfos = append(tmpInodeInfos, pTmpInodeInfo)
	}
	return tmpInodeInfos, ret
}

func (ns *nameSpace) DeleteDir(path string) int32 {
	var ret int32
	ret = 0

	if path == "/" {
		ret = 1
		return ret
	}

	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	var ok bool
	var pInodeInfo *mp.InodeInfo

	if ok, pInodeInfo = ns.InodeDBGet(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return ret
	}
	if pInodeInfo.InodeType == true {
		ret = 1 /*EPERM*/
		return ret
	}
	if tmplen := len(pInodeInfo.ChildrenInodeIDs); tmplen > 0 {
		ret = 1 /*EPERM*/
		return ret
	}

	/*update patent inode info*/
	var pTmpParentInodeInfo *mp.InodeInfo
	ok, pTmpParentInodeInfo = ns.InodeDBGet(keys[keysNum-2])
	if !ok {
		return 1
	}
	for index, value := range pTmpParentInodeInfo.ChildrenInodeIDs {
		if value == pInodeInfo.InodeID {
			pTmpParentInodeInfo.ChildrenInodeIDs = append(pTmpParentInodeInfo.ChildrenInodeIDs[:index], pTmpParentInodeInfo.ChildrenInodeIDs[index+1:]...)
			break
		}
	}
	tmpKey := strconv.FormatInt(pTmpParentInodeInfo.InodeID, 10)
	ns.InodeDBSet(tmpKey, pTmpParentInodeInfo)
	ns.InodeEtcdSet(tmpKey, ns.VolID, pTmpParentInodeInfo)

	ns.InodeDBSet(keys[keysNum-2], pTmpParentInodeInfo)

	/*delete inode info*/
	tmpKey = strconv.FormatInt(pInodeInfo.InodeID, 10)
	ns.InodeDBDelete(tmpKey)
	ns.InodeEtcdDelete(tmpKey, ns.VolID)

	ns.InodeDBDelete(strconv.FormatInt(pInodeInfo.ParentInodeID, 10) + "-" + utils.GetSelfName(path))

	return ret
}

func (ns *nameSpace) Rename(path1 string, path2 string) int32 {
	var ret int32
	ret = 0

	if path1 == "/" || path2 == "/" {
		ret = 1
		return ret
	}

	key1s := ns.GetAllKeyByFullPath(path1)
	key1sNum := len(key1s)
	var pInodeInfo *mp.InodeInfo
	var ok bool
	if ok, pInodeInfo = ns.InodeDBGet(key1s[key1sNum-1]); !ok {
		ret = 17 /*ENOENT*/
		return ret
	}

	key2s := ns.GetAllKeyByFullPath(path2)
	key2sNum := len(key2s)

	var pParentInodeInfo2 *mp.InodeInfo

	if ok, pParentInodeInfo2 = ns.InodeDBGet(key2s[key2sNum-2]); !ok {
		ret = 2 /*ENOENT*/
		return ret
	}
	if pParentInodeInfo2.InodeType == true {
		ret = 1 /*EPERM*/
		return ret
	}
	if ok, _ = ns.InodeDBGet(key2s[key2sNum-1]); ok {
		ret = 17 /*ENOENT*/
		return ret
	}

	if key2s[key2sNum-2] != key1s[key1sNum-2] {
		return 1 /*EPERM*/
	}

	// delete old inode key
	tmpKey := strconv.FormatInt(pInodeInfo.InodeID, 10)
	ns.InodeDBDelete(tmpKey)
	ns.InodeEtcdDelete(tmpKey, ns.VolID)
	// delete old parentID + name key
	ns.InodeDBDelete(strconv.FormatInt(pInodeInfo.ParentInodeID, 10) + "-" + utils.GetSelfName(path1))

	// add a new parentID + name key
	name := utils.GetSelfName(path2)
	tmpInodeInfo := mp.InodeInfo{
		ParentInodeID:    pParentInodeInfo2.InodeID,
		InodeID:          pInodeInfo.InodeID,
		Name:             name,
		AccessTime:       pInodeInfo.AccessTime,
		ModifiTime:       pInodeInfo.ModifiTime,
		InodeType:        pInodeInfo.InodeType,
		FileSize:         pInodeInfo.FileSize,
		ChunkIDs:         pInodeInfo.ChunkIDs,
		ChildrenInodeIDs: pInodeInfo.ChildrenInodeIDs}

	// add a new inode key
	tmpKey = strconv.FormatInt(pInodeInfo.InodeID, 10)
	ns.InodeDBSet(tmpKey, &tmpInodeInfo)
	ns.InodeEtcdSet(tmpKey, ns.VolID, &tmpInodeInfo)

	tmpKey = strconv.FormatInt(pParentInodeInfo2.InodeID, 10) + "-" + name
	ns.InodeDBSet(tmpKey, &tmpInodeInfo)

	/*
		// modify parents inodeinfo if they are not same
		if key2s[key2sNum-2] != key1s[key1sNum-2] {
			// update patent1 inode info
			var pTmpParentInodeInfo1 *mp.InodeInfo
			_, pTmpParentInodeInfo1 = ns.InodeDBGet(key1s[key1sNum-2])
			for index, value := range pTmpParentInodeInfo1.ChildrenInodeIDs {
				if value == pInodeInfo.InodeID {
					pTmpParentInodeInfo1.ChildrenInodeIDs = append(pTmpParentInodeInfo1.ChildrenInodeIDs[:index], pTmpParentInodeInfo1.ChildrenInodeIDs[index+1:]...)
					break
				}
			}
			ns.InodeDBSet(strconv.FormatInt(pTmpParentInodeInfo1.InodeID, 10), pTmpParentInodeInfo1)
			ns.InodeDBSet(strconv.FormatInt(pTmpParentInodeInfo1.ParentInodeID, 10)+"-"+utils.GetParentName(path1), pTmpParentInodeInfo1)

			// update patent2 inode info
			pParentInodeInfo2.ChildrenInodeIDs = append(pParentInodeInfo2.ChildrenInodeIDs, pInodeInfo.InodeID)
			parent2Name := utils.GetParentName(path2)
			if parent2Name == "/" {
				ns.InodeDBSet("0", pParentInodeInfo2)
			} else {
				ns.InodeDBSet(strconv.FormatInt(pParentInodeInfo2.InodeID, 10), pParentInodeInfo2)
				tmpKey2 := strconv.FormatInt(pParentInodeInfo2.ParentInodeID, 10) + "-" + utils.GetParentName(path2)
				ns.InodeDBSet(tmpKey2, pParentInodeInfo2)
			}
		}
	*/

	return ret
}
func (ns *nameSpace) CreateFile(path string) int32 {

	var ret int32
	ret = 0

	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	var ok bool
	var pParentInodeInfo *mp.InodeInfo
	if ok, pParentInodeInfo = ns.InodeDBGet(keys[keysNum-2]); !ok {
		ret = 2 /*ENOENT*/
		return ret
	}

	if pParentInodeInfo.InodeType == true {
		ret = 1 /*EPERM*/
		return ret
	}

	if ok, _ = ns.InodeDBGet(keys[keysNum-1]); ok {
		ret = 17 /*ENOENT*/
		return ret
	}

	/*update inode info*/
	inodeID := ns.AllocateInodeID()
	name := utils.GetSelfName(path)
	tmpInodeInfo := mp.InodeInfo{
		ParentInodeID: pParentInodeInfo.InodeID,
		InodeID:       inodeID,
		Name:          name,
		AccessTime:    time.Now().Unix(),
		ModifiTime:    time.Now().Unix(),
		InodeType:     true}

	tmpKey := strconv.FormatInt(inodeID, 10)
	ns.InodeDBSet(tmpKey, &tmpInodeInfo)
	ns.InodeEtcdSet(tmpKey, ns.VolID, &tmpInodeInfo)

	tmpKey = strconv.FormatInt(pParentInodeInfo.InodeID, 10) + "-" + name
	ns.InodeDBSet(tmpKey, &tmpInodeInfo)

	/*update patent inode info*/
	pParentInodeInfo.ChildrenInodeIDs = append(pParentInodeInfo.ChildrenInodeIDs, inodeID)

	parentName := utils.GetParentName(path)
	if parentName == "/" {
		ns.InodeDBSet("0", pParentInodeInfo)
		ns.InodeEtcdSet("0", ns.VolID, pParentInodeInfo)

	} else {
		tmpKey = strconv.FormatInt(pParentInodeInfo.InodeID, 10)
		ns.InodeDBSet(tmpKey, pParentInodeInfo)
		ns.InodeEtcdSet(tmpKey, ns.VolID, pParentInodeInfo)

		tmpKey = strconv.FormatInt(pParentInodeInfo.ParentInodeID, 10) + "-" + utils.GetParentName(path)
		ns.InodeDBSet(tmpKey, pParentInodeInfo)
	}

	return ret
}

func (ns *nameSpace) DeleteFile(path string) int32 {
	var ret int32
	ret = 0
	if path == "/" {
		ret = 1
		return ret
	}
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	var ok bool
	var pInodeInfo *mp.InodeInfo
	if ok, pInodeInfo = ns.InodeDBGet(keys[keysNum-1]); !ok {
		ret = 0 /*ENOENT*/
		return ret
	}
	if pInodeInfo.InodeType == false {
		ret = 1 /*EPERM*/
		return ret
	}
	/*update patent inode info*/
	var pTmpParentInodeInfo *mp.InodeInfo
	ok, pTmpParentInodeInfo = ns.InodeDBGet(keys[keysNum-2])
	if !ok {
		return 1
	}
	for index, value := range pTmpParentInodeInfo.ChildrenInodeIDs {
		if value == pInodeInfo.InodeID {
			pTmpParentInodeInfo.ChildrenInodeIDs = append(pTmpParentInodeInfo.ChildrenInodeIDs[:index], pTmpParentInodeInfo.ChildrenInodeIDs[index+1:]...)
			break
		}
	}
	tmpKey := strconv.FormatInt(pTmpParentInodeInfo.InodeID, 10)
	ns.InodeDBSet(tmpKey, pTmpParentInodeInfo)
	ns.InodeEtcdSet(tmpKey, ns.VolID, pTmpParentInodeInfo)

	ns.InodeDBSet(keys[keysNum-2], pTmpParentInodeInfo)

	/*delete chunk info*/
	for _, value := range pInodeInfo.ChunkIDs {

		ok, chunkInfo := ns.ChunkDBGet(value)
		if !ok {
			continue
		}
		/*release bg cnt*/
		ns.ReleaseBlockGroup(chunkInfo.BlockGroupID)
		ns.ChunkDBDelete(value)
		ns.ChunkEtcdDelete(value, ns.VolID)
	}

	/*delete inode info*/
	tmpKey = strconv.FormatInt(pInodeInfo.InodeID, 10)
	ns.InodeDBDelete(tmpKey)
	ns.InodeEtcdDelete(tmpKey, ns.VolID)

	ns.InodeDBDelete(strconv.FormatInt(pInodeInfo.ParentInodeID, 10) + "-" + utils.GetSelfName(path))
	return ret
}
func (ns *nameSpace) AllocateChunk(path string) (int32, *mp.ChunkInfo) {
	var ret int32

	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	if ok, _ := ns.InodeDBGet(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return ret, nil
	}

	var chunkInfo = mp.ChunkInfo{}
	ret, _, blockGroup := ns.ChooseBlockGroup()

	if ret != 0 {
		return 28 /*ENOSPC*/, nil
	}
	chunkInfo.BlockGroupID = blockGroup.BlockGroupID
	chunkInfo.ChunkSize = 0
	chunkInfo.ChunkID = ns.AllocateChunkID()

	return 0, &chunkInfo

}

func (ns *nameSpace) GetFileChunks(path string) (int32, []*mp.ChunkInfo) {
	var ret int32
	var ok bool
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	var pTmpInodeInfo *mp.InodeInfo
	if ok, pTmpInodeInfo = ns.InodeDBGet(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return ret, nil
	}
	chunkInfos := make([]*mp.ChunkInfo, 0)

	for i := range pTmpInodeInfo.ChunkIDs {
		chunkID := pTmpInodeInfo.ChunkIDs[i]
		ok, tmpChunkInfo := ns.ChunkDBGet(chunkID)
		if !ok {
			continue
		}
		chunkInfos = append(chunkInfos, tmpChunkInfo)
	}

	return 0, chunkInfos

}

func (ns *nameSpace) SyncChunk(path string, chunkinfo *mp.ChunkInfo) int32 {
	var ret int32
	var ok bool
	keys := ns.GetAllKeyByFullPath(path)
	keysNum := len(keys)

	var pTmpInodeInfo *mp.InodeInfo
	if ok, pTmpInodeInfo = ns.InodeDBGet(keys[keysNum-1]); !ok {
		ret = 2 /*ENOENT*/
		return ret
	}

	pTmpInodeInfo.ModifiTime = time.Now().Unix()

	var lastChunkInfo *mp.ChunkInfo
	var lastChunkID int64
	if len(pTmpInodeInfo.ChunkIDs) > 0 {
		// for appned write
		lastChunkID = pTmpInodeInfo.ChunkIDs[len(pTmpInodeInfo.ChunkIDs)-1]
		if lastChunkID == chunkinfo.ChunkID {
			ok, lastChunkInfo = ns.ChunkDBGet(chunkinfo.ChunkID)
			if !ok {
				return 1
			}
			pTmpInodeInfo.FileSize = pTmpInodeInfo.FileSize + int64(chunkinfo.ChunkSize) - int64(lastChunkInfo.ChunkSize)
		} else {
			pTmpInodeInfo.ChunkIDs = append(pTmpInodeInfo.ChunkIDs, chunkinfo.ChunkID)
			pTmpInodeInfo.FileSize += int64(chunkinfo.ChunkSize)
		}
	} else {
		pTmpInodeInfo.ChunkIDs = append(pTmpInodeInfo.ChunkIDs, chunkinfo.ChunkID)
		pTmpInodeInfo.FileSize += int64(chunkinfo.ChunkSize)
	}
	tmpKey := strconv.FormatInt(pTmpInodeInfo.InodeID, 10)
	ns.InodeDBSet(tmpKey, pTmpInodeInfo)
	ns.InodeDBSet(keys[keysNum-1], pTmpInodeInfo)
	ns.ChunkDBSet(chunkinfo.ChunkID, chunkinfo)

	go ns.ChunkEtcdSet(chunkinfo.ChunkID, ns.VolID, chunkinfo)
	go ns.InodeEtcdSet(tmpKey, ns.VolID, pTmpInodeInfo)

	return 0

}

func (ns *nameSpace) BlockGroupVp2Mp(in *vp.BlockGroup) *mp.BlockGroup {
	var mpBlockGroup = mp.BlockGroup{}

	mpBlockInfos := make([]*mp.BlockInfo, len(in.BlockInfos))

	mpBlockGroup.BlockGroupID = in.BlockGroupID
	mpBlockGroup.FreeCnt = in.FreeCnt
	mpBlockGroup.Status = in.Status

	for i := range in.BlockInfos {
		var pVpBlockInfo *vp.BlockInfo
		var mpBlockInfo mp.BlockInfo

		pVpBlockInfo = in.BlockInfos[i]
		mpBlockInfo.BlockID = pVpBlockInfo.BlockID
		mpBlockInfo.DataNodeIP = pVpBlockInfo.DataNodeIP
		mpBlockInfo.DataNodePort = pVpBlockInfo.DataNodePort
		mpBlockInfos[i] = &mpBlockInfo

	}

	mpBlockGroup.BlockInfos = mpBlockInfos
	return &mpBlockGroup

}

func (ns *nameSpace) ChooseBlockGroup() (int32, int32, *vp.BlockGroup) {

	var blockGroupID int32
	var blockGroup *vp.BlockGroup
	flag := false

	ns.BGMutex.RLock()
	for _, v := range ns.BlockGroupDB {
		if v.Status == 1 {
			blockGroupID = v.BlockGroupID
			v.FreeCnt = v.FreeCnt - 1
			if v.FreeCnt <= 0 {
				v.Status = 2
			}
			blockGroup = v
			logger.Debug("find a using blockgroup,blgid:%v\n", v.BlockGroupID)
			flag = true
			break
		}
	}
	// find the free blockgroup
	if flag == false {
		for _, v := range ns.BlockGroupDB {
			if v.Status == 0 {
				blockGroupID = v.BlockGroupID
				v.FreeCnt = v.FreeCnt - 1
				if v.FreeCnt == 0 {
					v.Status = 2
				} else {
					v.Status = 1
				}
				blockGroup = v
				logger.Debug("find a free blockgroup,blgid:%v\n", v.BlockGroupID)
				flag = true
				break
			}
		}
	}
	ns.BGMutex.RUnlock()

	if flag {
		ns.BlockGroupDBSet(blockGroupID, blockGroup)
		ns.BlockGroupEtcdSet(blockGroupID, ns.VolID, blockGroup)
		return 0, blockGroupID, blockGroup
	} else {
		return 1, -1, nil
	}

}

func (ns *nameSpace) ReleaseBlockGroup(blockGroupID int32) {

	ok, blockGroup := ns.BlockGroupDBGet(blockGroupID)
	if !ok {
		return
	}
	blockGroup.FreeCnt += 1
	if blockGroup.FreeCnt > 0 {
		blockGroup.Status = 1
	}
	if blockGroup.FreeCnt >= 160 {
		blockGroup.Status = 0
	}

	ns.BlockGroupDBSet(blockGroupID, blockGroup)
	ns.BlockGroupEtcdSet(blockGroupID, ns.VolID, blockGroup)

}
func (ns *nameSpace) GetAllKeyByFullPath(in string) (keys []string) {
	tmp := strings.Split(in, "/")
	keys = make([]string, 1)
	for i, v := range tmp {
		if i == 0 {
			keys[i] = "0"
		} else {
			if ok, pInodeInfo := ns.InodeDBGet(keys[i-1]); ok {
				buffer := new(bytes.Buffer)
				buffer.WriteString(strconv.FormatInt(pInodeInfo.InodeID, 10))
				buffer.WriteString("-")
				buffer.WriteString(v)
				//newkey := strconv.FormatInt(pInodeInfo.InodeID, 10) + "-" + v
				newkey := buffer.String()
				keys = append(keys, newkey)
			} else {
				keys = append(keys, " ")
			}
		}
	}

	return
}

func (ns *nameSpace) AllocateInodeID() int64 {
	id := ns.BaseInodeID.Id()
	ns.InodeBaseIDEtcdSet(strconv.FormatInt(id, 10), ns.VolID)
	return id
}

func (ns *nameSpace) AllocateChunkID() int64 {
	id := ns.BaseChunkID.Id()
	ns.ChunkBaseIDEtcdSet(strconv.FormatInt(id, 10), ns.VolID)
	return id
}

func (ns *nameSpace) InodeDBGet(k string) (bool, *mp.InodeInfo) {
	ns.Mutex.RLock()
	defer ns.Mutex.RUnlock()
	if v, ok := ns.InodeDB[k]; ok {
		return true, v
	} else {
		return false, nil
	}
}
func (ns *nameSpace) InodeDBSet(k string, v *mp.InodeInfo) {
	ns.Mutex.Lock()
	ns.InodeDB[k] = v
	ns.Mutex.Unlock()
}

func (ns *nameSpace) InodeDBDelete(k string) {
	ns.Mutex.Lock()
	delete(ns.InodeDB, k)
	ns.Mutex.Unlock()
}

func (ns *nameSpace) BlockGroupDBGet(k int32) (bool, *vp.BlockGroup) {
	ns.BGMutex.RLock()
	defer ns.BGMutex.RUnlock()
	if v, ok := ns.BlockGroupDB[k]; ok {
		return true, v
	} else {
		return false, nil
	}
}
func (ns *nameSpace) BlockGroupDBSet(k int32, v *vp.BlockGroup) {
	ns.BGMutex.Lock()
	ns.BlockGroupDB[k] = v
	ns.BGMutex.Unlock()
}

func (ns *nameSpace) BlockGroupDBDelete(k int32) {
	ns.BGMutex.Lock()
	delete(ns.BlockGroupDB, k)
	ns.BGMutex.Unlock()
}

func (ns *nameSpace) ChunkDBGet(k int64) (bool, *mp.ChunkInfo) {
	ns.CMutex.RLock()
	defer ns.CMutex.RUnlock()
	if v, ok := ns.ChunkDB[k]; ok {
		return true, v
	} else {
		return false, nil
	}
}
func (ns *nameSpace) ChunkDBSet(k int64, v *mp.ChunkInfo) {
	ns.CMutex.Lock()
	ns.ChunkDB[k] = v
	ns.CMutex.Unlock()
}

func (ns *nameSpace) ChunkDBDelete(k int64) {
	ns.CMutex.Lock()
	delete(ns.ChunkDB, k)
	ns.CMutex.Unlock()
}
