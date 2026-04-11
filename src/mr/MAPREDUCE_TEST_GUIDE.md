# MapReduce Lab 复现指南

## 1. 环境准备

### 1.1 Go 环境
```bash
go version  # 确认已安装 Go
```

### 1.2 进入项目目录
```bash
proj          # 如果配置了 proj alias
# 或
cd ~/Projects/active/6.5840
```

### 1.3 进入 src 目录
```bash
cd src
```

## 2. 运行测试

### 2.1 运行所有测试
```bash
make mr
```

### 2.2 运行特定测试
```bash
make mr RUN="-run TestWc"        # 只运行 TestWc
make mr RUN="-run TestIndexer"   # 只运行 TestIndexer
make mr RUN="-run TestMapParallel"
make mr RUN="-run TestReduceParallel"
make mr RUN="-run TestJobCount"
make mr RUN="-run TestEarlyExit"
make mr RUN="-run TestCrashWorker"
```

### 2.3 跳过 race 检测（Mac 上可能更稳定）
```bash
make mr RACE=
```

## 3. 测试说明

| 测试 | 说明 |
|------|------|
| TestWc | 词频统计测试 |
| TestIndexer | 索引生成测试 |
| TestMapParallel | 验证 Map 任务并行执行 |
| TestReduceParallel | 验证 Reduce 任务并行执行 |
| TestJobCount | 验证 Map 任务只执行一次 |
| TestEarlyExit | 验证所有 worker/coordinator 正常退出 |
| TestCrashWorker | 验证 worker 崩溃后任务能重新分配 |

## 4. 文件结构

```
src/
├── mr/
│   ├── coordinator.go  # Coordinator 实现
│   ├── worker.go      # Worker 实现
│   ├── rpc.go        # RPC 定义
│   └── mr_test.go    # 测试
├── mrapps/           # MapReduce 应用插件
└── main/            # 可执行文件
```

## 5. 预期输出

测试成功时最后会显示：
```
--- PASS: TestWc (x.xxs)
--- PASS: TestIndexer (x.xxs)
...
PASS
ok      6.5840/mr    xxx.xxxs
```
