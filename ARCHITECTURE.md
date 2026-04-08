# 6.5840 分布式系统实验架构文档

## 1. 项目概述

本项目是 MIT 6.5840 (原 6.824) 分布式系统课程的实验代码库，包含多个逐层递进的分布式系统实验：

- **Lab 1**: MapReduce 分布式计算框架
- **Lab 2**: Raft 共识算法
- **Lab 3**: 基于 Raft 的 Key-Value 服务 (kvraft)
- **Lab 4**: 分片 Key-Value 服务 (shardkv)

## 2. 目录结构

```
6.5840/
├── Makefile                 # 提交打包脚本
├── src/
│   ├── go.mod              # Go 模块定义
│   ├── Makefile            # 实验构建与测试
│   │
│   ├── main/               # 可执行入口
│   │   ├── mrcoordinator.go    # MR Coordinator
│   │   ├── mrworker.go         # MR Worker
│   │   ├── mrsequential.go     # MR 顺序执行
│   │   ├── kvsrv1d.go          # KV Server daemon
│   │   ├── raft1d.go           # Raft daemon
│   │   ├── kvraft1d.go         # KV Raft daemon
│   │   ├── rsm1d.go            # RSM daemon
│   │   └── shardgrp1d.go       # Shard Group daemon
│   │
│   ├── labgob/             # Gob 编码封装
│   ├── labrpc/             # 模拟 RPC 网络
│   ├── tester1/            # 测试框架
│   │   ├── config.go       # 测试配置
│   │   ├── group.go        # Server Group 管理
│   │   ├── daemonsrv.go    # Daemon 进程管理
│   │   └── persister.go    # 持久化封装
│   │
│   ├── mr/                 # MapReduce 实验
│   ├── mrapps/             # MR 应用程序插件
│   │
│   ├── raftapi/            # Raft 接口定义
│   ├── raft1/              # Raft 实现
│   │   ├── raft.go         # 核心协议实现
│   │   ├── server.go       # Server 桥接层
│   │   └── proxy.go        # RPC 代理
│   │
│   ├── kvsrv1/             # 单机 KV 服务
│   │   ├── server.go
│   │   ├── client.go
│   │   └── rpc/rpc.go      # KV RPC 协议
│   │
│   ├── kvraft1/            # 基于 Raft 的 KV 服务
│   │   ├── server.go
│   │   ├── client.go
│   │   ├── test.go
│   │   └── rsm/            # 复制状态机层
│   │       └── rsm.go
│   │
│   ├── shardkv1/           # 分片 KV 服务
│   │   ├── client.go
│   │   ├── test.go
│   │   ├── shardctrler/    # 分片控制器
│   │   ├── shardcfg/       # 分片配置
│   │   └── shardgrp/       # 分片组实现
│   │       ├── server.go
│   │       ├── client.go
│   │       └── shardrpc/   # 分片迁移 RPC
│   │
│   ├── kvtest1/            # KV 测试工具
│   └── models1/            # Porcupine 模型
│
└── ARCHITECTURE.md         # 本文件
```

## 3. 核心基础设施

### 3.1 labrpc - 模拟 RPC 网络

`src/labrpc/labrpc.go` 提供了一个可模拟网络故障的 RPC 框架：

- **Network**: 模拟网络，支持丢包、延迟、断连、乱序
- **ClientEnd**: 客户端端点，用于发送 RPC
- **Server**: RPC 服务端，可注册多个 Service
- **关键特性**:
  - `Reliable(bool)`: 设置网络是否可靠
  - `Enable(endname, enabled)`: 启用/禁用连接
  - `Connect(endname, servername)`: 连接客户端到服务端

```go
// 使用示例
net := labrpc.MakeNetwork()
end := net.MakeEnd("end1")
net.AddServer("server1", server)
net.Connect("end1", "server1")
net.Enable("end1", true)
```

### 3.2 tester1 - 测试框架

`src/tester1/` 是整个实验体系的核心测试框架：

- **config.go**: 测试配置管理，创建模拟网络环境
- **group.go**: Server Group 生命周期管理，支持网络分区
- **daemonsrv.go**: 将每个 Server 作为独立子进程运行
- **persister.go**: Raft 状态和 Snapshot 持久化封装

### 3.3 labgob - 编码封装

`src/labgob/labgob.go` 包装了 Gob 编码，检查 RPC/持久化对象的字段合法性。

## 4. 实验架构详解

### 4.1 MapReduce (Lab 1)

**独立技术栈**，不依赖 labrpc/tester1：

```
mrcoordinator → Unix Socket RPC → mrworkers
                ↓
            mrapps/*.so (plugin)
```

- **Coordinator**: 任务调度、Worker 管理
- **Worker**: 执行 Map/Reduce 任务
- **RPC**: 标准库 `net/rpc` + Unix Socket
- **Plugin**: 动态加载 Map/Reduce 函数

**关键文件**:
- `src/mr/coordinator.go`: 协调器实现
- `src/mr/worker.go`: Worker 实现
- `src/mr/rpc.go`: RPC 定义

### 4.2 Raft (Lab 2)

Raft 共识算法实现，为上层提供复制状态机基础：

```
┌─────────────────────────────────────┐
│           raft1/raft.go             │
│  ┌─────────┐  ┌─────────┐          │
│  │  Leader │  │ Follower│  ...     │
│  │Election │  │ Log Repl│          │
│  └────┬────┘  └────┬────┘          │
│       └─────────────┘               │
│              │                      │
│         applyCh (ApplyMsg)          │
└──────────────┼──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│      raft1/server.go (rfsrv)        │
│    桥接 Raft 与 tester 框架          │
└─────────────────────────────────────┘
```

**核心组件**:
- `raft.go`: Raft 状态机、选举、日志复制
- `server.go`: 从 applyCh 消费提交结果，与 tester 交互
- `proxy.go`: tester 到 daemon 的 RPC 代理

**接口定义** (`raftapi/raftapi.go`):
```go
type Raft interface {
    Start(command interface{}) (int, int, bool)
    GetState() (int, bool)
    Snapshot(index int, snapshot []byte)
    PersistBytes() int
}
```

### 4.3 KV Raft (Lab 3)

基于 Raft 的复制 KV 服务：

```
┌─────────────────────────────────────────┐
│           kvraft1/server.go             │
│         (业务层: Get/Put)               │
└─────────────────┬───────────────────────┘
                  │ DoOp/Snapshot/Restore
                  ▼
┌─────────────────────────────────────────┐
│         kvraft1/rsm/rsm.go              │
│      通用复制状态机 (RSM) 层             │
│   Submit → Raft → applyCh → Execute     │
└─────────────────┬───────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────┐
│            raft1/raft.go                │
│           Raft 共识层                    │
└─────────────────────────────────────────┘
```

**关键设计**:
- **RSM (Replicated State Machine)**: 抽象出通用的复制状态机框架
- **StateMachine 接口**: `DoOp`, `Snapshot`, `Restore`
- **Clerk**: 客户端，处理 leader 发现和重试

### 4.4 Sharded KV (Lab 4)

分片 Key-Value 服务，支持动态分片迁移：

```
                         ┌─────────────────┐
                         │  shardctrler    │
                         │ (配置控制器)     │
                         │ 存储于 kvsrv1    │
                         └────────┬────────┘
                                  │ Query/ChangeConfig
                                  ▼
┌─────────────────────────────────────────────────────────────┐
│                      shardkv1/client.go                      │
│                    (Clerk: 路由到正确 Group)                  │
│  key → shardcfg.Key2Shard() → 查配置 → gid → group servers   │
└─────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
   │  shardgrp   │     │  shardgrp   │     │  shardgrp   │
   │  Group 1    │     │  Group 2    │     │  Group 3    │
   │             │     │             │     │             │
   │ ┌─────────┐ │     │ ┌─────────┐ │     │ ┌─────────┐ │
   │ │Shard 0-3│ │     │ │Shard 4-7│ │     │ │Shard 8-11│ │
   │ │ (RSM)   │ │     │ │ (RSM)   │ │     │ │ (RSM)   │ │
   │ └─────────┘ │     │ └─────────┘ │     │ └─────────┘ │
   └─────────────┘     └─────────────┘     └─────────────┘
```

**核心组件**:

1. **shardcfg**: 分片配置模型
   - `NShards = 12`: 固定分片数
   - `Key2Shard(key)`: key 到分片的映射
   - `Rebalance()`: 重新平衡分片分配

2. **shardctrler**: 配置控制器
   - 管理分片到 Group 的映射
   - 状态存储在 Group 0 的 kvsrv1 中

3. **shardgrp**: 分片组实现
   - 每个 Group 包含多个副本 (通过 Raft 复制)
   - 支持 shard 冻结、安装、删除 (用于迁移)
   - 复用 `kvraft1/rsm` 作为复制层

4. **shardrpc**: 分片迁移协议
   - `FreezeShard`: 冻结分片停止写入
   - `InstallShard`: 安装分片数据
   - `DeleteShard`: 删除已迁移的分片

## 5. 模块依赖关系

```
labgob
  │
  ▼
labrpc ◄────────────────────────────┐
  │                                  │
  ▼                                  │
tester1 ◄───────────────────────────┤
  │                                  │
  ├──► raft1 ◄──┬──► kvraft1/rsm ◄──┼──► kvraft1
  │             │                    │
  │             └────────────────────┼──► shardgrp ◄──► shardkv1
  │                                  │
  ├──► kvsrv1 ◄──────────────────────┼──► shardctrler
  │                                  │
  └──► kvtest1 ◄──► models1 ◄───────┤──► porcupine (外部依赖)
                                    │
mr (独立体系) ◄─────────────────────┘
```

## 6. 运行与测试

### 6.1 构建与测试

```bash
# 在 src/ 目录下

# 测试所有实验
make all

# 单独测试
make mr         # MapReduce
make kvsrv1     # 单机 KV
make raft1      # Raft
make rsm1       # RSM 层
make kvraft1    # KV Raft
make shardkv    # 分片 KV

# 指定测试用例
make RUN="-run TestBasicAgree" raft1
```

### 6.2 提交打包

```bash
# 在项目根目录
make lab1    # MapReduce
make lab2    # Raft
make lab3a   # KV Raft A
...
```

## 7. 关键代码入口

| 功能 | 文件路径 |
|------|----------|
| Raft 实现 | `src/raft1/raft.go` |
| Raft 接口 | `src/raftapi/raftapi.go` |
| RSM 层 | `src/kvraft1/rsm/rsm.go` |
| KV RPC 协议 | `src/kvsrv1/rpc/rpc.go` |
| 分片配置 | `src/shardkv1/shardcfg/shardcfg.go` |
| 分片控制器 | `src/shardkv1/shardctrler/shardctrler.go` |
| 分片组 | `src/shardkv1/shardgrp/server.go` |
| 线性一致性检查 | `src/kvtest1/porcupine.go` |
| 测试框架 | `src/tester1/config.go` |

## 8. 架构特点

1. **分层设计**: 从单机 KV → Raft 复制 → 分片，逐层递进
2. **RSM 抽象**: `kvraft1/rsm` 提供通用的复制状态机框架
3. **Daemon 进程模型**: 每个 Server 作为独立子进程运行，更接近真实系统
4. **统一测试框架**: `tester1` 提供网络模拟、故障注入、持久化测试
5. **线性一致性验证**: 集成 Porcupine 进行形式化验证
6. **MapReduce 独立**: 使用标准库 RPC，与其他实验解耦

## 9. 实现状态说明

- **基础设施**: `labgob`, `labrpc`, `tester1` 已完整实现
- **测试框架**: 完整的测试和验证体系
- **实验骨架**: 部分实验代码为模板状态 (含 `Your code here` 注释)
- **残留入口**: `src/main/` 下存在历史实验入口 (如 `diskvd.go`)，但依赖目录缺失，不属于当前主线
