# 演示说明

## 演示步骤

1. 进入项目目录：

```bash
cd submission/
```

2. 创建并激活虚拟环境，安装依赖：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# (Optional) If your pip supports editable installs, you can also do:
# pip install -e .
```

3. 复制 `.env.example` 为 `.env` 并设置 `POLYGON_RPC_URL`（也兼容 `RPC_URL` / `POLYGON_RPC`）。

4. 初始化数据库：

```bash
python -m src.main init-db
```

## 预期输出

- A new SQLite file at `DB_PATH` (default: `./polyreputation.sqlite`)

## 可选步骤（推荐）

5. 同步 markets（用于 tokenId → market 的映射，更容易把交易归类到具体 market）：

```bash
python -m src.main sync-traded-markets --batch 5 --max-token-ids 2000
```

6. 回填链上真实交易（目标至少 >= 100 条 `OrderFilled`，并写入 SQLite）。

先用**交易哈希定位正确的区块号**（避免盲扫区块范围一直 `logs=0`）：

```bash
python -m src.main inspect-tx --tx-hash 0x916cad96dd5c219997638133512fd17fe7c1ce72b830157e4fd5323cf4f19946
```

记下输出里的 `block_number = B`，然后在 \(B\) 附近回填：

```bash
python -m src.main backfill-trades --start-block <B-20000> --end-block <B+20000> --chunk 500 --stop-after 150 --sleep-ms 100
```

- `trades` table contains decoded `OrderFilled` logs
- You should aim for **>= 100** trades for the hackathon requirement

Verify count (optional):

```bash
python -c "import sqlite3, os; p=os.getenv('DB_PATH','./polyreputation.sqlite'); c=sqlite3.connect(p); print(c.execute('select count(*) from trades').fetchone()[0])"
```

7. 计算画像/标签/排行榜：

```bash
python -m src.main compute
```

8. 启动 API 和 Streamlit：

```bash
# Terminal 1
python -m src.main serve-api

# Terminal 2
streamlit run src/streamlit_app.py
```

## 预期输出（本地）

- API: open `http://127.0.0.1:8000/leaderboard`
- Streamlit: Leaderboard + Following + Discover (Signal Finder) + Profile + proof JSON viewer

Add screenshots to `screenshots/` and optionally include a short video in `video/` (<=5 minutes).

## 截图
- 将运行截图放入 `screenshots/`（建议最少包含：leaderboard、profile、API 返回示例）

## 演示数据
- Market: `will-there-be-another-us-government-shutdown-by-january-31`
- Tx: `0x916cad96dd5c219997638133512fd17fe7c1ce72b830157e4fd5323cf4f19946`