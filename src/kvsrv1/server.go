package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 单机 KV 服务器
// 存储 key-value 数据，带版本号
type KVServer struct {
	mu   sync.Mutex
	data map[string]Entry // 存储 key -> (value, version)
}

// 存储条目
type Entry struct {
	Value   string
	Version rpc.Tversion
}

// 创建 KVServer
func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.data = make(map[string]Entry)
	return kv
}

// Get 返回 key 的值和版本。
// 如果 key 不存在，返回 ErrNoKey。
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// 在这里实现
}

// Put 更新 key 的值（需要版本匹配）。
// - 版本匹配 → 更新，返回 OK
// - 版本不匹配 → 返回 ErrVersion
// - key 不存在且 Version=0 → 插入，返回 OK
// - key 不存在且 Version!=0 → 返回 ErrNoKey
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// 在这里实现
}

// 启动 KVServer（忽略参数，用于复制 KV 服务）
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
