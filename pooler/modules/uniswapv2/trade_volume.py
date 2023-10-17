import time

from redis import asyncio as aioredis

from .utils.core import get_pair_trade_volume
from .utils.models.message_models import UniswapTradesSnapshot
from pooler.utils.callback_helpers import GenericProcessorSnapshot
from pooler.utils.default_logger import logger
from pooler.utils.models.message_models import EpochBaseSnapshot
from pooler.utils.rpc import RpcHelper


class TradeVolumeProcessor(GenericProcessorSnapshot):
    transformation_lambdas = None

    def __init__(self) -> None:
        """
    Initializes the TradeVolumeProcessor class.

    Args:
        None

    Attributes:
        transformation_lambdas (list): A list of transformation lambdas to be applied to the processed epoch data.
        _logger (Logger): A logger object for logging messages related to the TradeVolumeProcessor module.
    """
        self.transformation_lambdas = [
            self.transform_processed_epoch_to_trade_volume,
        ]
        self._logger = logger.bind(module='TradeVolumeProcessor')

    async def compute(
        self,
        min_chain_height: int,
        max_chain_height: int,
        data_source_contract_address: str,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
    ):
        """
    Computes the trade volume for a given data source contract address within a specified range of chain heights.

    Args:
        min_chain_height (int): The minimum chain height to consider for the computation.
        max_chain_height (int): The maximum chain height to consider for the computation.
        data_source_contract_address (str): The address of the data source contract.
        redis_conn (aioredis.Redis): The connection to the Redis database.
        rpc_helper (RpcHelper): An instance of the RpcHelper class.

    Returns:
        The computed trade volume for the specified data source contract address.

    Example:
        ```
        result = await compute(
            min_chain_height=100,
            max_chain_height=200,
            data_source_contract_address="0x1234567890abcdef",
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )
        ```

    """
        self._logger.debug(f'trade volume {data_source_contract_address}, computation init time {time.time()}')
        result = await get_pair_trade_volume(
            data_source_contract_address=data_source_contract_address,
            min_chain_height=min_chain_height,
            max_chain_height=max_chain_height,
            redis_conn=redis_conn,
            rpc_helper=rpc_helper,
        )
        self._logger.debug(f'trade volume {data_source_contract_address}, computation end time {time.time()}')
        return result

    def transform_processed_epoch_to_trade_volume(
        self,
        snapshot,
        data_source_contract_address,
        epoch_begin,
        epoch_end,
    ):
        """
    This function takes in a processed snapshot of trade data and transforms it into a trade volume snapshot. It sets the effective trade volume at the top level by extracting relevant information from the input snapshot. It then creates a new instance of the `UniswapTradesSnapshot` class with the extracted data and returns it.

    Parameters:
    - `snapshot` (dict): The processed snapshot of trade data.
    - `data_source_contract_address` (str): The contract address of the data source.
    - `epoch_begin` (int): The beginning of the epoch.
    - `epoch_end` (int): The end of the epoch.

    Returns:
    - `trade_volume_snapshot` (UniswapTradesSnapshot): The transformed trade volume snapshot.

    Note: The `UniswapTradesSnapshot` class is assumed to be defined elsewhere.
    """
        self._logger.debug(
            'Trade volume processed snapshot: {}', snapshot,
        )

        # Set effective trade volume at top level
        total_trades_in_usd = snapshot['Trades'][
            'totalTradesUSD'
        ]
        total_fee_in_usd = snapshot['Trades']['totalFeeUSD']
        total_token0_vol = snapshot['Trades'][
            'token0TradeVolume'
        ]
        total_token1_vol = snapshot['Trades'][
            'token1TradeVolume'
        ]
        total_token0_vol_usd = snapshot['Trades'][
            'token0TradeVolumeUSD'
        ]
        total_token1_vol_usd = snapshot['Trades'][
            'token1TradeVolumeUSD'
        ]

        max_block_timestamp = snapshot.get('timestamp')
        snapshot.pop('timestamp', None)
        trade_volume_snapshot = UniswapTradesSnapshot(
            contract=data_source_contract_address,
            chainHeightRange=EpochBaseSnapshot(begin=epoch_begin, end=epoch_end),
            timestamp=max_block_timestamp,
            totalTrade=float(f'{total_trades_in_usd: .6f}'),
            totalFee=float(f'{total_fee_in_usd: .6f}'),
            token0TradeVolume=float(f'{total_token0_vol: .6f}'),
            token1TradeVolume=float(f'{total_token1_vol: .6f}'),
            token0TradeVolumeUSD=float(f'{total_token0_vol_usd: .6f}'),
            token1TradeVolumeUSD=float(f'{total_token1_vol_usd: .6f}'),
            events=snapshot
        )
        return trade_volume_snapshot
