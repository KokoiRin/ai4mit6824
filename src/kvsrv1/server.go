package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Entry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]Entry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.data = make(map[string]Entry)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if entry, ok := kv.data[args.Key]; ok {
		reply.Value = entry.Value
		reply.Version = entry.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if entry, ok := kv.data[args.Key]; ok {
		// key 存在，检查版本
		if entry.Version != args.Version {
			reply.Err = rpc.ErrVersion
		} else {
			// 版本匹配，更新
			kv.data[args.Key] = Entry{
				Value:   args.Value,
				Version: entry.Version + 1,
			}
			reply.Err = rpc.OK
		}
	} else {
		// key 不存在
		if args.Version == 0 {
			// Version=0，插入新值
			kv.data[args.Key] = Entry{
				Value:   args.Value,
				Version: 1,
			}
			reply.Err = rpc.OK
		} else {
			// Version!=0，返回 ErrNoKey
			reply.Err = rpc.ErrNoKey
		}
	}
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
