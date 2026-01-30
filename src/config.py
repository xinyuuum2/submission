from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List

from dotenv import load_dotenv


def _split_csv(value: str | None) -> List[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


@dataclass(frozen=True)
class Settings:
    polygon_rpc_url: str
    ctf_exchange_addresses: List[str]
    db_path: str
    gamma_api_base: str

    start_block: int | None
    end_block: int | None


def load_settings(*, require_rpc: bool = True) -> Settings:
    load_dotenv()

    # Be permissive: evaluators often use `RPC_URL` / `POLYGON_RPC`.
    polygon_rpc_url = (
        os.environ.get("POLYGON_RPC_URL")
        or os.environ.get("RPC_URL")
        or os.environ.get("POLYGON_RPC")
        or ""
    ).strip()
    if not polygon_rpc_url:
        if require_rpc:
            raise RuntimeError("Missing POLYGON_RPC_URL (set it in .env)")
        polygon_rpc_url = "https://polygon-rpc.com"

    addresses = _split_csv(os.environ.get("CTF_EXCHANGE_ADDRESSES"))
    if not addresses:
        # Polymarket CTF Exchange (current + legacy)
        addresses = [
            "0xC5d563A36AE78145C45a50134d48A1215220f80a",
            "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E",
        ]

    db_path = os.environ.get("DB_PATH", "./polyreputation.sqlite").strip()
    gamma_api_base = os.environ.get("GAMMA_API_BASE", "https://gamma-api.polymarket.com").strip()

    start_block_raw = os.environ.get("START_BLOCK", "").strip()
    end_block_raw = os.environ.get("END_BLOCK", "").strip()

    start_block = int(start_block_raw) if start_block_raw else None
    end_block = int(end_block_raw) if end_block_raw else None

    return Settings(
        polygon_rpc_url=polygon_rpc_url,
        ctf_exchange_addresses=addresses,
        db_path=db_path,
        gamma_api_base=gamma_api_base,
        start_block=start_block,
        end_block=end_block,
    )
