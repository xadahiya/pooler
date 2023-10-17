from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis

from ..utils.models.message_models import UniswapPairTotalReservesSnapshot
from ..utils.models.message_models import UniswapStatsSnapshot
from ..utils.models.message_models import UniswapTradesAggregateSnapshot
from pooler.utils.callback_helpers import GenericProcessorMultiProjectAggregate
from pooler.utils.data_utils import get_project_epoch_snapshot
from pooler.utils.data_utils import get_sumbmission_data_bulk
from pooler.utils.data_utils import get_tail_epoch_id
from pooler.utils.default_logger import logger
from pooler.utils.models.message_models import PowerloomCalculateAggregateMessage
from pooler.utils.rpc import RpcHelper


class AggreagateStatsProcessor(GenericProcessorMultiProjectAggregate):
    transformation_lambdas = None

    def __init__(self) -> None:
        """
    Initializes an instance of the AggregateStatsProcessor class.

    Args:
        None

    Attributes:
        transformation_lambdas (list): A list to store transformation lambdas.
        _logger (Logger): A logger object for logging messages related to the AggregateStatsProcessor module.
    """
        self.transformation_lambdas = []
        self._logger = logger.bind(module='AggregateStatsProcessor')

    async def compute(
        self,
        msg_obj: PowerloomCalculateAggregateMessage,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        project_id: str,

    ):
        """
    Computes the statistics for a given PowerloomCalculateAggregateMessage.

    Args:
        msg_obj (PowerloomCalculateAggregateMessage): The message object containing the necessary data for computation.
        redis (aioredis.Redis): The Redis client for data retrieval.
        rpc_helper (RpcHelper): The helper class for making RPC calls.
        anchor_rpc_helper (RpcHelper): The helper class for making anchor RPC calls.
        ipfs_reader (AsyncIPFSClient): The IPFS client for reading data.
        protocol_state_contract: The protocol state contract.
        project_id (str): The ID of the project.

    Returns:
        UniswapStatsSnapshot: The computed statistics snapshot.

    Raises:
        None

    This function computes the statistics for a given PowerloomCalculateAggregateMessage. It retrieves the necessary data from Redis, IPFS, and makes RPC calls to calculate the statistics. The computed statistics are returned as a UniswapStatsSnapshot object.

    The function first logs the calculation process. Then, it retrieves the submission data in bulk using the Redis client, IPFS client, and the project IDs from the message object. It creates an empty dictionary to store the snapshot mapping.

    Next, it iterates over the messages and snapshot data to populate the snapshot mapping dictionary. If the data is not available, it skips the iteration. If the project ID contains 'reserves', it parses the data as UniswapPairTotalReservesSnapshot. If the project ID contains 'volume', it parses the data as UniswapTradesAggregateSnapshot and appends the snapshot's completeness flag to the complete_flags list. The snapshot mapping dictionary is updated with the project ID as the key and the snapshot as the value.

    After populating the snapshot mapping, the function initializes the stats_data dictionary with the required keys and initial values. It then iterates over the snapshot mapping to calculate the statistics. If the project ID contains 'reserves', it calculates the total value locked (TVL) by summing the token0ReservesUSD and token1ReservesUSD values for the maximum epoch block. If the project ID contains 'volume', it adds the totalTrade and totalFee values to the volume24h and fee24h respectively.

    Next, the function retrieves the tail epoch ID and extrapolated flag using the Redis client, protocol state contract, anchor RPC helper, epoch ID, time range, and project ID. If the extrapolated flag is False, it retrieves the previous stats snapshot data using the Redis client, protocol state contract, anchor RPC helper, IPFS client, tail epoch ID, and project ID.

    If the previous stats snapshot data is available, it parses it as UniswapStatsSnapshot. It then calculates the change in percentage for volume24h, TVL, and fee24h by subtracting the current values from the previous snapshot's values, dividing by the previous snapshot's values, and multiplying by 100.

    Finally, the function creates a new UniswapStatsSnapshot object with the computed statistics and assigns it to the stats_snapshot variable. If any of the complete_flags is False, it sets the complete flag of the stats_snapshot to False.

    The function returns the stats_snapshot object as the computed statistics.

    Note: This function is an asynchronous function and should be awaited when called.

    """
        self._logger.info(f'Calculating unswap stats for {msg_obj}')

        epoch_id = msg_obj.epochId

        snapshot_mapping = {}

        snapshot_data = await get_sumbmission_data_bulk(
            redis, [msg.snapshotCid for msg in msg_obj.messages], ipfs_reader, [
                msg.projectId for msg in msg_obj.messages
            ],
        )

        complete_flags = []
        for msg, data in zip(msg_obj.messages, snapshot_data):
            if not data:
                continue
            if 'reserves' in msg.projectId:
                snapshot = UniswapPairTotalReservesSnapshot.parse_obj(data)
            elif 'volume' in msg.projectId:
                snapshot = UniswapTradesAggregateSnapshot.parse_obj(data)
                complete_flags.append(snapshot.complete)
            snapshot_mapping[msg.projectId] = snapshot

        stats_data = {
            'volume24h': 0,
            'tvl': 0,
            'fee24h': 0,
            'volumeChange24h': 0,
            'tvlChange24h': 0,
            'feeChange24h': 0,
        }
        # iterate over all snapshots and generate stats data
        for snapshot_project_id in snapshot_mapping.keys():
            snapshot = snapshot_mapping[snapshot_project_id]

            if 'reserves' in snapshot_project_id:
                max_epoch_block = snapshot.chainHeightRange.end

                stats_data['tvl'] += snapshot.token0ReservesUSD[f'block{max_epoch_block}'] + \
                    snapshot.token1ReservesUSD[f'block{max_epoch_block}']

            elif 'volume' in snapshot_project_id:
                stats_data['volume24h'] += snapshot.totalTrade
                stats_data['fee24h'] += snapshot.totalFee

        # source project tail epoch
        tail_epoch_id, extrapolated_flag = await get_tail_epoch_id(
            redis, protocol_state_contract, anchor_rpc_helper, msg_obj.epochId, 86400, project_id,
        )
        if not extrapolated_flag:
            previous_stats_snapshot_data = await get_project_epoch_snapshot(
                redis, protocol_state_contract, anchor_rpc_helper, ipfs_reader, tail_epoch_id, project_id,
            )

            if previous_stats_snapshot_data:
                previous_stats_snapshot = UniswapStatsSnapshot.parse_obj(previous_stats_snapshot_data)

                # calculate change in percentage
                stats_data['volumeChange24h'] = (stats_data['volume24h'] - previous_stats_snapshot.volume24h) / \
                    previous_stats_snapshot.volume24h * 100

                stats_data['tvlChange24h'] = (stats_data['tvl'] - previous_stats_snapshot.tvl) / \
                    previous_stats_snapshot.tvl * 100

                stats_data['feeChange24h'] = (stats_data['fee24h'] - previous_stats_snapshot.fee24h) / \
                    previous_stats_snapshot.fee24h * 100

        stats_snapshot = UniswapStatsSnapshot(
            epochId=epoch_id,
            volume24h=stats_data['volume24h'],
            tvl=stats_data['tvl'],
            fee24h=stats_data['fee24h'],
            volumeChange24h=stats_data['volumeChange24h'],
            tvlChange24h=stats_data['tvlChange24h'],
            feeChange24h=stats_data['feeChange24h'],
        )

        if not all(complete_flags):
            stats_snapshot.complete = False

        return stats_snapshot
