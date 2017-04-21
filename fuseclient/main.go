package main

import (
	"fmt"
	"log"
	"math"
	"os"
	//	"strconv"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/lxmgo/config"
	"golang.org/x/net/context"
	//"math/rand"

	cfs "github.com/bwits/containerfs/fs"
	"github.com/bwits/containerfs/logger"
	mp "github.com/bwits/containerfs/proto/mp"
	//	"github.com/bwits/containerfs/utils"
)

var uuid string
var mountPoint string

type FS struct {
	cfs *cfs.CFS
}

type node interface {
	fs.Node
	setName(name string)
}

type refcount struct {
	node node
	refs uint32
}

type Dir struct {
	mu     sync.Mutex
	fs     *FS
	name   string // root to this dir
	inode  *mp.InodeInfo
	active map[string]*refcount // for fuse rename update f.name immediately , otherwise f.name will be old name after rename in about 30s
}
type File struct {
	mu      sync.Mutex
	parent  *Dir
	name    string
	writers uint
	handles uint32
	cfile   *cfs.CFile
	inode   *mp.InodeInfo
}

func main() {

	c, err := config.NewConfig(os.Args[1])
	if err != nil {
		fmt.Println("NewConfig err")
		os.Exit(1)
	}
	uuid = c.String("uuid")
	mountPoint = c.String("mountpoint")
	cfs.VolMgrAddr = c.String("volmgr")
	cfs.EtcdEndPoints = c.Strings("etcd")
	//cfs.MetaNodeAddr = c.String("metanode")

	logger.SetConsole(true)
	logger.SetRollingFile(c.String("log"), "fuse.log", 10, 100, logger.MB) //each 100M rolling
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

	cfs.GetMetaLeader()
	go cfs.ChooseMetaLeaderWatcher()

	time.Sleep(time.Second * 1)

	err = mount(uuid, mountPoint)
	if err != nil {
		log.Fatal(err)
	}
}

func mount(uuid, mountPoint string) error {
	cfs := cfs.OpenFileSystem(uuid)
	c, err := fuse.Mount(
		mountPoint,
		fuse.MaxReadahead(128*1024),
		fuse.AsyncRead(),
		fuse.WritebackCache(),
		fuse.FSName("ContainerFS-"+uuid),
		fuse.LocalVolume(),
		fuse.VolumeName("ContainerFS-"+uuid))
	if err != nil {
		return err
	}
	defer c.Close()

	filesys := &FS{
		cfs: cfs,
	}
	if err := fs.Serve(c, filesys); err != nil {
		return err
	}
	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		return err
	}

	return nil
}

var _ = fs.FS(&FS{})

func (fs *FS) Root() (fs.Node, error) {
	n := newDir(fs, nil, "/")
	return n, nil
}

/*
   Blocks  uint64 // Total data blocks in file system.
   Bfree   uint64 // Free blocks in file system.
   Bavail  uint64 // Free blocks in file system if you're not root.
   Files   uint64 // Total files in file system.
   Ffree   uint64 // Free files in file system.
   Bsize   uint32 // Block size
   Namelen uint32 // Maximum file name length?
   Frsize  uint32 // Fragment size, smallest addressable data size in the file system.
*/
func (fs *FS) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	err, ret := cfs.GetFSInfo(fs.cfs.VolID)
	if err != 0 {
		return fuse.Errno(syscall.EIO)
	}
	resp.Bsize = 64 * 1024 * 1024
	resp.Frsize = resp.Bsize
	resp.Blocks = ret.TotalSpace / (64 * 1024 * 1024)
	resp.Bfree = ret.FreeSpace / (64 * 1024 * 1024)
	resp.Bavail = ret.FreeSpace / (64 * 1024 * 1024)
	return nil
}

var _ node = (*Dir)(nil)
var _ = fs.Node(&Dir{})

func newDir(filesys *FS, inode *mp.InodeInfo, name string) *Dir {
	d := &Dir{
		inode:  inode,
		name:   name,
		fs:     filesys,
		active: make(map[string]*refcount),
	}
	return d
}

func (d *Dir) reviveDir(inode *mp.InodeInfo, name string) (*Dir, error) {
	child := newDir(d.fs, inode, name)
	return child, nil
}

func (d *Dir) setName(name string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.name = name
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0755
	return nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	var res []fuse.Dirent
	// todo : only need list name,not all inodeinfo
	ret, inodes := d.fs.cfs.List(d.name)
	if ret == 2 {
		return nil, fuse.Errno(syscall.ENOENT)
	}
	if ret != 0 {
		return nil, fuse.Errno(syscall.EIO)
	}
	for _, v := range inodes {
		de := fuse.Dirent{
			Name: v.Name,
		}
		if v.InodeType {
			de.Type = fuse.DT_File
		} else {
			de.Type = fuse.DT_Dir
		}
		res = append(res, de)
	}
	return res, nil
}

var _ = fs.NodeStringLookuper(&Dir{})

func (d *Dir) reviveNode(inode *mp.InodeInfo, name string, fullpath string) (node, error) {
	if inode.InodeType {
		child := &File{
			name:   name,
			parent: d,
			inode:  inode,
		}

		return child, nil
	} else {
		child, _ := d.reviveDir(inode, fullpath)
		return child, nil
	}

}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if a, ok := d.active[name]; ok {
		return a.node, nil
	}
	var fullPath string
	if d.name == "/" {
		fullPath = d.name + name
	} else {
		fullPath = d.name + "/" + name
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	ret, inode := d.fs.cfs.Stat(fullPath)
	if ret == 2 {
		return nil, fuse.ENOENT
	}
	if ret != 0 {
		return nil, fuse.ENOENT
	}
	n, _ := d.reviveNode(inode, name, fullPath)
	a := &refcount{node: n}
	if inode.InodeType {
		d.active[name] = a
	} else {
		d.active[fullPath] = a
	}
	return n, nil
}

var _ = fs.NodeMkdirer(&Dir{})

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	var fullPath string

	if d.name == "/" {
		fullPath = d.name + req.Name
	} else {
		fullPath = d.name + "/" + req.Name
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	ret := d.fs.cfs.CreateDir(fullPath)
	if ret == -1 {
		return nil, fuse.Errno(syscall.EIO)
	}
	if ret == 1 {
		return nil, fuse.Errno(syscall.EPERM)
	}
	if ret == 2 {
		return nil, fuse.Errno(syscall.ENOENT)
	}
	if ret == 17 {
		return nil, fuse.Errno(syscall.EEXIST)
	}

	ret, inode := d.fs.cfs.Stat(fullPath)
	child := newDir(d.fs, inode, fullPath)
	d.active[fullPath] = &refcount{node: child}

	return child, nil
}

var _ = fs.NodeCreater(&Dir{})

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {

	logger.Debug("Create...,Flag:%v", req.Flags)

	var fullPath string
	if d.name == "/" {
		fullPath = d.name + req.Name
	} else {
		fullPath = d.name + "/" + req.Name
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	ret, cfile := d.fs.cfs.CreateFile(fullPath, int(req.Flags))
	if ret != 0 {
		if ret == 17 {
			return nil, nil, fuse.Errno(syscall.EEXIST)

		} else {
			return nil, nil, fuse.Errno(syscall.EIO)

		}
	}
	ret, inode := d.fs.cfs.Stat(fullPath)
	child := &File{
		inode:   inode,
		name:    req.Name,
		parent:  d,
		cfile:   cfile,
		handles: 1,
		writers: 1,
	}

	d.active[req.Name] = &refcount{node: child}

	return child, child, nil
}

var _ = fs.NodeRemover(&Dir{})

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	var fullPath string

	if d.name == "/" {
		fullPath = d.name + req.Name
	} else {
		fullPath = d.name + "/" + req.Name
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if req.Dir {

		ret := d.fs.cfs.DeleteDir(fullPath)
		if ret != 0 {
			if ret == 2 {
				return fuse.Errno(syscall.EPERM)
			} else {
				return fuse.Errno(syscall.EIO)
			}
		}
	} else {
		ret := d.fs.cfs.DeleteFile(fullPath)
		if ret != 0 {
			if ret == 2 {
				return fuse.Errno(syscall.EPERM)
			} else {
				return fuse.Errno(syscall.EIO)
			}
		}
	}

	if req.Dir {
		if a, ok := d.active[fullPath]; ok {
			delete(d.active, fullPath)
			a.node.setName("")
		}
	} else {
		if a, ok := d.active[req.Name]; ok {
			delete(d.active, req.Name)
			a.node.setName("")
		}
	}

	return nil
}

var _ = fs.NodeRenamer(&Dir{})

func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	if newDir != d {
		return fuse.Errno(syscall.EPERM)
	}

	newDirInstant := newDir.(*Dir)

	var fullPath1 string
	var fullPath2 string

	if d.name == "/" {
		fullPath1 = d.name + req.OldName
	} else {
		fullPath1 = d.name + "/" + req.OldName
	}

	_, inode := d.fs.cfs.Stat(fullPath1)
	if !inode.InodeType {
		return fuse.Errno(syscall.EPERM)
	}

	if newDirInstant.name == "/" {
		fullPath2 = newDirInstant.name + req.NewName
	} else {
		fullPath2 = newDirInstant.name + "/" + req.NewName
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	ret := d.fs.cfs.Rename(fullPath1, fullPath2)
	if ret != 0 {
		if ret == 2 {
			return fuse.Errno(syscall.ENOENT)
		} else if ret == 1 || ret == 17 {
			return fuse.Errno(syscall.EPERM)
		} else {
			return fuse.Errno(syscall.EIO)
		}
	}

	ret, inodeNew := d.fs.cfs.Stat(fullPath2)

	if inodeNew.InodeType {
		// tell overwritten node it's unlinked
		if a, ok := d.active[req.NewName]; ok {
			a.node.setName("")
		}

		// if the source inode is active, record its new name
		if aOld, ok := d.active[req.OldName]; ok {
			aOld.node.setName(req.NewName)
			delete(d.active, req.OldName)
			d.active[req.NewName] = aOld
		}
	} else {
		// tell overwritten node it's unlinked
		if a, ok := d.active[fullPath2]; ok {
			a.node.setName("")
		}

		// if the source inode is active, record its new name
		if aOld, ok := d.active[fullPath1]; ok {
			aOld.node.setName(fullPath2)
			delete(d.active, fullPath1)
			d.active[fullPath2] = aOld
		}
	}

	return nil
}

var _ node = (*File)(nil)
var _ = fs.Node(&File{})
var _ = fs.Handle(&File{})

func (f *File) setName(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.name = name
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	var fullPath string
	if f.parent.name == "/" {
		fullPath = f.parent.name + f.name
	} else {
		fullPath = f.parent.name + "/" + f.name
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	ret, inode := f.parent.fs.cfs.Stat(fullPath)
	if ret != 0 {
		return nil
	}

	a.Ctime = time.Unix(inode.ModifiTime, 0)
	a.Mtime = time.Unix(inode.ModifiTime, 0)
	a.Atime = time.Unix(inode.AccessTime, 0)
	a.Size = uint64(inode.FileSize)
	a.Inode = uint64(inode.InodeID)

	a.BlockSize = 128 * 1024 // this is for fuse attr quick update
	a.Blocks = uint64(math.Ceil(float64(a.Size) / float64(a.BlockSize)))
	a.Mode = 0666

	return nil
}

var _ = fs.NodeOpener(&File{})

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	var ret int32

	var fullPath string

	logger.Debug("OpenFlag:%v", req.Flags)

	if f.parent.name == "/" {
		fullPath = f.parent.name + f.name
	} else {
		fullPath = f.parent.name + "/" + f.name
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.writers > 0 {
		if int(req.Flags)&cfs.O_WRONLY != 0 || int(req.Flags)&cfs.O_RDWR != 0 {
			return nil, fuse.Errno(syscall.EPERM)
		}
	}

	if f.cfile == nil && f.handles == 0 {
		ret, f.cfile = f.parent.fs.cfs.OpenFile(fullPath, int(req.Flags))
		if ret != 0 {
			return nil, fuse.Errno(syscall.EIO)
		}
	} else {
		f.parent.fs.cfs.UpdateOpenFile(f.cfile, int(req.Flags))
	}

	tmp := f.handles + 1
	f.handles = tmp

	if int(req.Flags)&cfs.O_WRONLY != 0 || int(req.Flags)&cfs.O_RDWR != 0 {
		tmp := f.writers + 1
		f.writers = tmp
	}

	resp.Flags = fuse.OpenDirectIO
	return f, nil
}

var _ = fs.HandleReleaser(&File{})

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	logger.Debug("Release...")

	f.mu.Lock()
	defer f.mu.Unlock()

	f.cfile.Close(int(req.Flags))
	f.handles--

	if int(req.Flags)&cfs.O_WRONLY != 0 || int(req.Flags)&cfs.O_RDWR != 0 {
		f.writers--
	}

	if f.handles == 0 {
		f.cfile.DestroyChannel()
		f.cfile = nil
	}

	return nil
}

var _ = fs.HandleReader(&File{})

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.cfile.ReaderMap[req.Handle]; !ok {
		rdinfo := cfs.ReaderInfo{}
		rdinfo.LastOffset = int64(0)
		f.cfile.ReaderMap[req.Handle] = &rdinfo
	}
	if req.Offset == f.cfile.FileSize {
		return nil
	}

	length := f.cfile.Read(req.Handle, &resp.Data, req.Offset, int64(req.Size))
	if length != int64(req.Size) {
		logger.Error("== Read reqsize:%v, but return datasize:%v ==\n", req.Size, length)
	}
	if length < 0 {
		return fuse.Errno(syscall.EIO)
	}
	return nil
}

var _ = fs.HandleWriter(&File{})

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {

	f.mu.Lock()
	defer f.mu.Unlock()
	w := f.cfile.Write(req.Data, int32(len(req.Data)))
	if w != int32(len(req.Data)) {
		if w == -1 {
			return fuse.Errno(syscall.ENOSPC)
		} else {
			return fuse.Errno(syscall.EIO)
		}
	}
	resp.Size = int(w)
	return nil
}

var _ = fs.HandleFlusher(&File{})

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	return nil
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

var _ = fs.NodeSetattrer(&File{})

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	return nil
}
