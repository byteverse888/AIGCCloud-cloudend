# 算力贡献积分激励结算模块

独立的 Python 模块，通过 cron 定时调用，统计节点在线/Pod 负载并计算积分奖励，每日批量清算转账到联盟链。

## 模块结构

```
incentive/
├── __init__.py          # 模块声明
├── __main__.py          # python -m incentive 入口
├── main.py              # CLI 入口（子命令分发）
├── config.py            # 配置管理（环境变量 + .env）
├── logger.py            # 日志（轮转文件 + 控制台）
├── gpu_database.py      # GPU 型号性能数据库
├── k8s_client.py        # K8s REST API 分页查询
├── parse_client.py      # Parse Server 同步客户端
├── web3_client.py       # Web3 联盟链批量转账
├── calculator.py        # 积分计算引擎（核心公式）
├── collector.py         # 每小时节点统计采集
├── settlement.py        # 每日积分清算 + 批量转账
├── query.py             # 节点/全网查询接口
└── requirements.txt     # 依赖（仅 httpx）
```

## 快速开始

> **重要**：所有命令均需在项目根目录 `aigccloud-cloudend/` 下执行，而非 `incentive/` 子目录内。
> `python -m incentive` 依赖 Python 的包查找机制，必须从包含 `incentive/` 的父目录运行。

```
aigccloud-cloudend/          ← 在此目录下执行所有命令
└── incentive/               ← 本模块
    ├── .venv/               ← 虚拟环境（自动生成）
    ├── .env                 ← 环境变量配置（从 .env.example 复制）
    ├── .env.example         ← 配置模板
    ├── __init__.py
    ├── __main__.py
    └── ...
```

### 安装依赖

```bash
cd aigccloud-cloudend

mv incentive ~/AIGC/  # 单独的目录
cd ~/AIGC

# 在 incentive 目录内创建虚拟环境
python3 -m venv incentive/.venv
source incentive/.venv/bin/activate

# 安装依赖
pip install -r incentive/requirements.txt
```

### 配置

将 `.env.example` 复制为 `.env` 并填入实际值：

```bash
cp incentive/.env.example incentive/.env
```

主要参数：

```bash
# K8s（二选一）
# 方式1: kubeconfig 文件（推荐，留空则自动查找 ~/.kube/config）
K8S_KUBECONFIG=/path/to/kubeconfig
# 方式2: token 直连
K8S_API_SERVER=https://127.0.0.1:6443
K8S_TOKEN=your-k8s-token

# Parse Server
PARSE_SERVER_URL=http://localhost:1337/parse
PARSE_APP_ID=BTGAPPId
PARSE_MASTER_KEY=your-master-key

# Web3 联盟链
WEB3_RPC_URL=http://localhost:8545
WEB3_CHAIN_ID=888
INCENTIVE_WALLET_PRIVATE_KEY=0x...
```

完整配置项见 `config.py`。

### 命令行用法

```bash
# 每小时统计（cron 每小时调用）
python -m incentive collect

# 每日清算（cron 每天凌晨 2 点调用）
python -m incentive settle

# 查询全网信息
python -m incentive network

# 查询节点信息
python -m incentive node 0x1234abcd...
# 查询节点积分历史
python -m incentive history 0x1234abcd... --limit 50
```

### Cron 配置

```cron
# 每10分钟采集节点数据并计算积分
*/10 * * * *  cd /home/ubuntu/AIGC/aigccloud-cloudend && incentive/.venv/bin/python -m incentive collect

# 每小时整点执行清算转账
0 * * * *  cd /home/ubuntu/AIGC/aigccloud-cloudend && incentive/.venv/bin/python -m incentive settle

# 每小时采集节点数据并计算积分
0 * * * *  cd /home/ubuntu/AIGC/aigccloud-cloudend && incentive/.venv/bin/python -m incentive collect

# 每天凌晨 2 点执行清算转账
0 2 * * *  cd /home/ubuntu/AIGC/aigccloud-cloudend && incentive/.venv/bin/python -m incentive settle

```

## 积分计算公式

### 贡献积分

```
Pc = B_base × hours × M_online × D_cont × GPU_score/100 × VRAM_w × GPU_count_f × M_t × S_t + Pod_bonus
```

| 参数 | 说明 |
|------|------|
| `B_base` | 节点基线分（端侧=5, 轻量=10, 超级=20） |
| `hours` | 本周期在线时长 |
| `M_online` | 在线奖励倍率（前24h=1×, 24-168h=1.5×, 168-720h=2×） |
| `D_cont` | 连续在线系数（0-24h=1.0, 24-72h=1.2, 72-168h=1.5, 168h+=1.8） |
| `GPU_score` | GPU 算力分（H100=100, RTX4090=65, RTX3090=45 等） |
| `VRAM_w` | 显存权重（0.1~1.5） |
| `GPU_count_f` | 多卡系数（1 + (N-1)×0.8） |
| `M_t` | 网络成熟度系数（<100节点=2.0, 100-1000=1.5, >1000=1.0, >10000=0.8） |
| `S_t` | 算力短缺系数（供需比驱动，0.8~2.0） |
| `Pod_bonus` | Pod 任务加成（每 Pod 额外 base×0.5，最多 10 个） |

### 掉线恢复机制

| 掉线时长 | 处理 |
|----------|------|
| < 5 分钟 | 不清零（宽限期） |
| 5-30 分钟 | 连续在线时长衰减 25% |
| > 30 分钟 | 连续在线时长清零 |

### 可兑换积分

用户付费的 80% 按兑换率转化为可兑换积分，按节点算力比例分配。

## 数据存储

### Parse User 表新增字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `totalContribution` | Number | 总贡献积分（累加） |
| `exchangeableBalance` | Number | 可兑换积分余额 |
| `pendingSettlement` | Number | 待清算积分 |
| `continuousOnlineHours` | Number | 连续在线时长（小时） |
| `nodeCoefficient` | Number | 硬件节点系数（GPU算力分×显存权重×多卡系数） |
| `lastSeenAt` | String | 最后在线时间（ISO，用于掉线检测） |

### IncentiveLog 表

| 字段 | 类型 | 说明 |
|------|------|------|
| `userId` | String | 用户 objectId |
| `web3Address` | String | ETH 地址 |
| `type` | String | 日志类型（online_reward / settlement） |
| `amount` | Number | 积分数额 |
| `description` | String | 描述/计算公式 |
| `settlementStatus` | String | unsettled / settled |
| `txHash` | String | 链上交易哈希 |
| `batchId` | String | 清算批次 ID |

### Parse Config（全网统计）

| 键 | 说明 |
|----|------|
| `networkTotalNodes` | 全网总节点 |
| `networkOnlineNodes` | 在线节点数 |
| `networkTotalGPU` | 总 GPU 数 |
| `networkRunningPods` | 运行中 Pod 数 |
| `networkSupplyDemandRatio` | 供需比 |
| `networkMaturityFactor` | 网络成熟度系数 |
| `networkShortageFactor` | 算力短缺系数 |
| `networkUtilization` | 算力利用率 |
| `networkTotalContributionIssued` | 总发放贡献积分 |
| `networkLastUpdated` | 最后更新时间 |

## 日志

- 位置：`incentive/logs/incentive.log`
- 轮转：50MB × 5 文件
- 保留：30 天
- 级别：INFO（可通过 `INCENTIVE_LOG_LEVEL` 配置）

## 注意事项

- 本模块**完全独立**，不依赖主应用代码，唯一外部依赖为 `httpx`
- Web3 RPC URL 为空时自动进入 mock 模式（模拟转账），适用于开发测试
- K8s 节点名格式约定：`0x{ETH地址}-{注册时间戳}`
- 积分清算最低转账额度默认 1000，可通过 `INCENTIVE_MIN_TRANSFER` 配置
