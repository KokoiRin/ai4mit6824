package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

// Clerk 是客户端代理，发 RPC 请求到 KVServer
type Clerk struct {
	clnt   *tester.Clnt
	server string
}

// 创建 Clerk
func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	return ck
}

// Get 获取 key 的值和版本。
// 如果 key 不存在，返回 ErrNoKey。
// 其他错误会一直重试直到成功。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// 在这里实现
}

// Put 更新 key 的值（需要版本匹配）。
// - 版本匹配 → 更新成功，返回 OK
// - 版本不匹配 → 返回 ErrVersion
// - 首次重试收到 ErrVersion → 返回 ErrVersion
// - 再次重试收到 ErrVersion → 返回 ErrMaybe（之前那次可能成功了）
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// 在这里实现
}
