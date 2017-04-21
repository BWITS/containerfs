package utils

import (
	"errors"
	"github.com/coreos/etcd/clientv3"
	"github.com/bwits/containerfs/logger"
	"golang.org/x/net/context"
)

type EtcdV3 struct {
	Client *clientv3.Client
	ctx    context.Context
}

func (etcdcli *EtcdV3) ttlOpts(ctx context.Context, ttl int64) ([]clientv3.OpOption, error) {
	if ttl == 0 {
		return nil, nil
	}
	// put keys within into same lease. We shall benchmark this and optimize the performance.
	lcr, err := etcdcli.Client.Lease.Grant(ctx, ttl)
	if err != nil {
		return nil, err
	}
	return []clientv3.OpOption{clientv3.WithLease(clientv3.LeaseID(lcr.ID))}, nil
}
func notFound(key string) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(key), "=", 0)
}

func (etcdcli *EtcdV3) InitEtcd(etcdServerList []string) error {
	cfg := clientv3.Config{
		Endpoints: etcdServerList,
	}
	etcdClient, err := clientv3.New(cfg)
	if err != nil {
		return err
	}
	etcdcli.Client = etcdClient
	etcdcli.ctx = context.Background()
	return nil
}

// Get implements storage.Interface.Get.
func (etcdcli *EtcdV3) Get(key string, recursive bool) (*clientv3.GetResponse, error) {

	var getResp *clientv3.GetResponse
	var err error

	if recursive {
		getResp, err = etcdcli.Client.KV.Get(etcdcli.ctx, key, clientv3.WithPrefix())
	} else {
		getResp, err = etcdcli.Client.KV.Get(etcdcli.ctx, key)
	}

	if err != nil {
		return nil, err
	}
	if len(getResp.Kvs) == 0 {
		return nil, errors.New("key not found")
	}

	return getResp, nil
}

func (etcdcli *EtcdV3) Set(key string, val string) error {

	if _, err := etcdcli.Client.KV.Put(etcdcli.ctx, key, val); err != nil {
		logger.Error("set etcd err:", err)
		return errors.New("set etcd err")
	}
	return nil

}

func (etcdcli *EtcdV3) DoDelete(key string) error {
	if _, err := etcdcli.Client.KV.Delete(etcdcli.ctx, key); err != nil {
		logger.Error("delete etcd err:", err)
		return errors.New("delete etcd err")
	}
	return nil
}
