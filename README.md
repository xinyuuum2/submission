# PolyBook

## 项目简介
PolyBook 是一个把 **Polymarket（Polygon）链上成交记录**做成「社交笔记」的数据产品：你可以发现值得关注的链上交易者、订阅他们的动态，并且随时用 on-chain proof 验证每一条“笔记”。

- **交易事实（Proof）**：从 Polygon 的 Polymarket CTF Exchange 合约回填 `OrderFilled` 事件，保存 `tx_hash + log_index + decoded JSON` 作为可验证证据
- **市场元数据**：通过 Gamma API 同步 market 与 `tokenId → market` 映射，并用 `outcomePrices` 做一个简单的“赢家 token”推断
- **聚合计算**：对已 resolved 的市场计算近似 realized PnL，产出 `user_market_pnl / user_stats / user_tags`
- **社交层（本地 MVP）**：Follow / Swipe 等社交行为保存在本地 SQLite（演示友好，链上数据仍可验证）

## 技术架构
- **数据获取**：Polygon RPC `eth_getLogs` 拉取 CTF Exchange 合约 `OrderFilled` 事件
- **事件解码**：按 `OrderFilled` ABI 解码 + 推断 side/tokenId/price 等字段
- **数据存储**：SQLite（`trades/markets/token_map/user_market_pnl/user_stats/user_tags`）
- **服务层**：FastAPI 提供查询接口（leaderboard/profile）
- **展示层**：Streamlit 看板（Leaderboard + Following + Discover + About；Profile 从页面内进入）

### 目录结构（核心）

- `src/cli.py`：命令入口（init-db / backfill-trades / sync-*-markets / compute / serve-api）
- `src/index_trades.py`：链上 `OrderFilled` 回填与解码
- `src/gamma.py`：Gamma markets 同步与 tokenId 映射
- `src/compute.py`：PnL/榜单/标签计算（SQLite CTE）
- `src/api_app.py`：FastAPI（leaderboard/profile）
- `src/streamlit_app.py`：Streamlit 看板

## 快速开始

### 环境要求
- 有效的 Polygon RPC URL（推荐你自己的 RPC；公共 RPC 可能会限流/限制 logs 查询范围）

### 安装步骤
1. 克隆仓库 / 解压后进入项目目录（如果你解压后看到的目录名就是 `submission/`，直接进入该目录即可）：

```bash
cd submission/
```

2. 安装依赖：

> 本项目使用 `src/` 布局。评审环境可直接用 `python -m src.main ...` 运行（**无需**设置 `PYTHONPATH`）。

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# （可选）如果你的 pip 支持 editable install，也可以：
# pip install -e .
```

3. 配置环境变量：

```bash
cp .env.example .env
# 编辑 .env，把 `POLYGON_RPC_URL` 换成你自己的 RPC（必需）
# 兼容：也支持 `RPC_URL` / `POLYGON_RPC`
```

4. 运行项目（初始化数据库）：

```bash
python -m src.main init-db
```

### Demo 跑法

完整步骤见 `DEMO.md`（包含：如何回填并验证至少 100 条链上真实交易记录）。

### API（本地）

- `GET /health`
- `GET /leaderboard?sort=roi|profit&limit=50`
- `GET /profile/{address}`

### Streamlit（本地）

```bash
streamlit run src/streamlit_app.py
```

页面包含：
- **Leaderboard（🏆 影响力榜单）**：社交口吻的数据看板 + 表格样式增强（标签化 sector、带单收益等）
- **Following（订阅动态）**：把 watchlist 变成信息流（Notes/Log/Cards/Table），每条动态可一键验证
- **Discover（发现好博主）**：刷卡式“博主名片”（人设标签 + bento 磁贴 + 可读证据流），一键订阅
- **Profile**：地址画像 + tags + proof JSON（**不在左侧导航**，通过各页的 Profile 按钮进入）
- **About**：项目说明与数据来源

> UI 主题：默认深色主题（见 `.streamlit/config.toml`）。

### 功能说明（对应最低门槛）

- **交易解码**：解析 Polygon 上 Polymarket CTF Exchange 的 `OrderFilled` 事件，推断 BUY/SELL、tokenId、成交额与价格，并把 `tx_hash + log_index + decoded_json` 入库作为 proof
- **市场识别**：通过 Gamma API 同步 `tokenId → market` 映射（`token_map`），把 trade 归类到具体市场（`markets`）
- **数据存储**：SQLite（`trades/markets/token_map/user_market_pnl/user_stats/user_tags`）
- **API 接口**：FastAPI 提供可查询的 HTTP 端点（leaderboard/profile）
- **前端展示**：Streamlit 看板（Leaderboard + Following + Discover + About；Profile 从页面内进入）

### Discover 模块（创新加分项 / 工具化 UI）

- **刷卡式发现**：随机抽取一批高 Signal 的潜在博主（可换一批），用“名片”展示关键数据与人设
- **订阅（本地）**：Follow 写入 SQLite 的 `user_follows`，用于生成订阅信息流（MVP：本地 watchlist）
- **互动记录（本地）**：Skip/Follow 写入 `dating_swipes`（便于去重与复现）
- **Signal score（快速筛选）**：用于 Demo 的全局信号评分 + 可解释的链上证据

### UI 说明（便于评审理解）

- **Profile 入口**：为了简化导航，Profile 不显示在左侧导航栏；通过 Leaderboard/Following/Discover 内的 “Profile” 按钮进入，并支持返回。
- **user_XXXXXX**：Leaderboard 默认显示本地生成的用户 id（由地址稳定映射，便于观看），**不是 Polymarket 官方账号 id**；进入 Profile 会展示完整地址与链上 proof。

### 数据来源

- **交易数据（必须）**：Polygon RPC `eth_getLogs` 拉取 CTF Exchange 合约 `OrderFilled` logs（真实链上数据）
- **市场元数据（辅助）**：Gamma API（`/markets`）用于 market 详情、outcome/tokenId 映射与 winner 推断

### 常见问题（运行时）

- **为什么 slug/market_id 是 n/a**：说明还没把对应 tokenId 的 Gamma market 元数据同步到本地；运行 `python -m src.main sync-traded-markets`（不要加 `--closed-only`）即可逐步补齐。
- **Gamma 同步很慢/超时（ProxyError）**：检查并临时关闭代理环境变量（`http_proxy/https_proxy` 等），再重试同步。

### 重要假设/限制（MVP）

- **USDC 精度**：默认按 6 decimals 处理（展示时会换算成 USDC）
- **赢家推断**：用 Gamma 的 `outcomePrices` 最大值且 \(\ge 0.99\) 近似判断赢家 token（用于 resolved 市场）
- **PnL 范围**：只对 resolved 市场做近似 realized PnL（不是完整的持仓/未实现收益会计）

### 团队成员（可选）

（在此处补充你的名字/队友）
