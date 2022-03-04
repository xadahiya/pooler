from pydantic import BaseModel, validator
from typing import Union, List, Optional


class TimeoutConfig(BaseModel):
    basic: int
    archival: int
    connection_init: int


class RLimitConfig(BaseModel):
    file_descriptors: int


class RPCLogsQueryConfig(BaseModel):
    chunk: int
    retry: int


class RPCConfig(BaseModel):
    matic: List[str]
    eth_mainnet: str
    retry: int
    logs_query: RPCLogsQueryConfig


class SystemConfig(BaseModel):
    polymarket_strapi_url: str
    rpc: RPCConfig
    rlimit: RLimitConfig


class liquidityProcessedData(BaseModel):
    contractAddress: str
    name: str
    liquidity: str
    volume_24h: str
    volume_7d: str
    cid_volume_24h: str
    cid_volume_7d: str
    fees_24h: str
    block_height: int
    deltaToken0Reserves: float
    deltaToken1Reserves: float
    deltaTime: float
    latestTimestamp: float
    earliestTimestamp: float