package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)


type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	return ck
}

// Get fetches the current value and version for a key.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		args := &rpc.GetArgs{Key: key}
		reply := &rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", args, reply)
		if ok {
			return reply.Value, reply.Version, reply.Err
		}
	}
}

// Put updates key with value only if the version matches.
// Returns ErrMaybe if RPC had to be retried.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	for attempt := 0; ; attempt++ {
		args := &rpc.PutArgs{
			Key:     key,
			Value:   value,
			Version: version,
		}
		reply := &rpc.PutReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Put", args, reply)
		if !ok {
			// RPC 失败，需要重试
			continue
		}
		if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
			return reply.Err
		}
		if reply.Err == rpc.ErrVersion {
			if attempt == 0 {
				// 第一次收到 ErrVersion，直接返回
				return rpc.ErrVersion
			}
			// 重试后仍收到 ErrVersion，说明之前那次成功了
			return rpc.ErrMaybe
		}
	}
}
