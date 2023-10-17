import asyncio

import pydantic
from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis

from ..utils.models.message_models import UniswapTradesAggregateSnapshot
from pooler.modules.uniswapv2.utils.helpers import get_pair_metadata
from pooler.utils.callback_helpers import GenericProcessorSingleProjectAggregate
from pooler.utils.data_utils import get_project_epoch_snapshot
from pooler.utils.data_utils import get_submission_data
from pooler.utils.data_utils import get_tail_epoch_id
from pooler.utils.default_logger import logger
from pooler.utils.models.message_models import PowerloomSnapshotSubmittedMessage
from pooler.utils.rpc import RpcHelper


class AggreagateTradeVolumeProcessor(GenericProcessorSingleProjectAggregate):
    transformation_lambdas = None

    def __init__(self) -> None:
        """
    Initializes an instance of the AggregateTradeVolumeProcessor7d class.

    Args:
        None

    Attributes:
        transformation_lambdas (list): A list to store transformation lambdas.
        _logger (Logger): A logger object for logging module information.
    """
        self.transformation_lambdas = []
        self._logger = logger.bind(module='AggregateTradeVolumeProcessor7d')

    def _add_aggregate_snapshot(
        self,
        previous_aggregate_snapshot: UniswapTradesAggregateSnapshot,
        current_snapshot: UniswapTradesAggregateSnapshot,
    ):
        """
    Adds the values of the current snapshot to the corresponding values of the previous aggregate snapshot.

    Args:
        previous_aggregate_snapshot (UniswapTradesAggregateSnapshot): The previous aggregate snapshot.
        current_snapshot (UniswapTradesAggregateSnapshot): The current snapshot.

    Returns:
        UniswapTradesAggregateSnapshot: The updated previous aggregate snapshot with the values of the current snapshot added.

    """

        previous_aggregate_snapshot.totalTrade += current_snapshot.totalTrade
        previous_aggregate_snapshot.totalFee += current_snapshot.totalFee
        previous_aggregate_snapshot.token0TradeVolume += current_snapshot.token0TradeVolume
        previous_aggregate_snapshot.token1TradeVolume += current_snapshot.token1TradeVolume
        previous_aggregate_snapshot.token0TradeVolumeUSD += current_snapshot.token0TradeVolumeUSD
        previous_aggregate_snapshot.token1TradeVolumeUSD += current_snapshot.token1TradeVolumeUSD

        return previous_aggregate_snapshot

    def _remove_aggregate_snapshot(
        self,
        previous_aggregate_snapshot: UniswapTradesAggregateSnapshot,
        current_snapshot: UniswapTradesAggregateSnapshot,
    ):
        """
    Removes the values of a given `UniswapTradesAggregateSnapshot` object from another `UniswapTradesAggregateSnapshot` object. The function subtracts the trade count, fee, trade volume of token0, trade volume of token1, trade volume of token0 in USD, and trade volume of token1 in USD from the corresponding values in the `current_snapshot` object and updates the values in the `previous_aggregate_snapshot` object. The updated `previous_aggregate_snapshot` object is then returned.
    """

        previous_aggregate_snapshot.totalTrade -= current_snapshot.totalTrade
        previous_aggregate_snapshot.totalFee -= current_snapshot.totalFee
        previous_aggregate_snapshot.token0TradeVolume -= current_snapshot.token0TradeVolume
        previous_aggregate_snapshot.token1TradeVolume -= current_snapshot.token1TradeVolume
        previous_aggregate_snapshot.token0TradeVolumeUSD -= current_snapshot.token0TradeVolumeUSD
        previous_aggregate_snapshot.token1TradeVolumeUSD -= current_snapshot.token1TradeVolumeUSD

        return previous_aggregate_snapshot

    async def compute(
        self,
        msg_obj: PowerloomSnapshotSubmittedMessage,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        project_id: str,

    ):
        """

    Computes a 7-day trade volume aggregate snapshot for a given project.

    Args:
        msg_obj (PowerloomSnapshotSubmittedMessage): The message object containing information about the snapshot.
        redis (aioredis.Redis): The Redis connection object.
        rpc_helper (RpcHelper): The helper object for making RPC calls.
        anchor_rpc_helper (RpcHelper): The helper object for making anchor RPC calls.
        ipfs_reader (AsyncIPFSClient): The IPFS client object for reading data.
        protocol_state_contract: The protocol state contract.
        project_id (str): The ID of the project.

    Returns:
        UniswapTradesAggregateSnapshot: The computed aggregate snapshot.

    Raises:
        None.

    This function computes a 7-day trade volume aggregate snapshot for a given project. It first fetches the metadata for the pair address using the `get_pair_metadata` function. Then, it creates an instance of `UniswapTradesAggregateSnapshot` with the provided epoch ID.

    Next, it fetches 24-hour aggregate snapshots spaced out by 1 day over 7 days. It starts by finding the tail epoch for the current epoch ID and queues a task for fetching the 24-hour aggregate snapshot for the project ID at the currently received epoch ID. It appends this task to the `snapshot_tasks` list.

    If the seek stop flag is False or the count is less than 7, it attempts to seek further back by finding the tail epoch ID and queues a task for fetching the 24-hour aggregate snapshot for the project ID at the rewinded epoch ID. This task is also appended to the `snapshot_tasks` list. The head epoch is updated to the tail epoch ID minus 1. This process continues until the seek stop flag is True or the count exceeds 7.

    If the count is equal to 7, it means that the function has reached the 7-day limit for fetching the 24-hour aggregate snapshots.

    All the snapshot tasks are then executed using `asyncio.gather` and the results are stored in the `all_snapshots` list.

    The function then iterates over each 24-hour snapshot in `all_snapshots` and checks if it is a valid snapshot. If it is, the snapshot is parsed and added to the `aggregate_snapshot` using the `_add_aggregate_snapshot` method.

    Finally, the function checks if all the snapshots are complete and if the count is less than 7. If any of these conditions are not met, the `complete` flag of the `aggregate_snapshot` is set to False.

    The computed aggregate snapshot is returned as the result of the function.

    """
        self._logger.info(f'Building 7 day trade volume aggregate snapshot against {msg_obj}')

        contract = project_id.split(':')[-2]

        pair_metadata = await get_pair_metadata(
            pair_address=contract,
            redis_conn=redis,
            rpc_helper=rpc_helper,
        )
        aggregate_snapshot = UniswapTradesAggregateSnapshot(
            epochId=msg_obj.epochId,
        )
        # 24h snapshots fetches
        snapshot_tasks = list()
        self._logger.debug('fetching 24hour aggregates spaced out by 1 day over 7 days...')
        # 1. find one day tail epoch
        count = 0
        self._logger.debug(
            'fetch # {}: queueing task for 24h aggregate snapshot for project ID {}'
            ' at currently received epoch ID {} with snasphot CID {}',
            count, msg_obj.projectId, msg_obj.epochId, msg_obj.snapshotCid,
        )

        snapshot_tasks.append(
            get_submission_data(
                redis, msg_obj.snapshotCid, ipfs_reader, msg_obj.projectId,
            ),
        )
        seek_stop_flag = False
        head_epoch = msg_obj.epochId
        # 2. if not extrapolated, attempt to seek further back
        while not seek_stop_flag or count < 7:
            tail_epoch_id, seek_stop_flag = await get_tail_epoch_id(
                redis, protocol_state_contract, anchor_rpc_helper, head_epoch, 86400, msg_obj.projectId,
            )
            count += 1
            if not seek_stop_flag or count > 1:
                self._logger.debug(
                    'fetch # {}: for 7d aggregated trade volume calculations: '
                    'queueing task for 24h aggregate snapshot for project ID {} at rewinded epoch ID {}',
                    count, msg_obj.projectId, tail_epoch_id,
                )
                snapshot_tasks.append(
                    get_project_epoch_snapshot(
                        redis, protocol_state_contract, anchor_rpc_helper,
                        ipfs_reader, tail_epoch_id, msg_obj.projectId,
                    ),
                )
            head_epoch = tail_epoch_id - 1
        if count == 7:
            self._logger.info(
                'fetch # {}: reached 7 day limit for 24h aggregate snapshots for project ID {} at rewinded epoch ID {}',
                count, msg_obj.projectId, tail_epoch_id,
            )
        all_snapshots = await asyncio.gather(*snapshot_tasks, return_exceptions=True)
        self._logger.debug(
            'for 7d aggregated trade volume calculations: fetched {} '
            '24h aggregated trade volume snapshots for project ID {}: {}',
            len(all_snapshots), msg_obj.projectId, all_snapshots,
        )
        complete_flags = []
        for single_24h_snapshot in all_snapshots:
            if not isinstance(single_24h_snapshot, BaseException):
                try:
                    snapshot = UniswapTradesAggregateSnapshot.parse_obj(single_24h_snapshot)
                    complete_flags.append(snapshot.complete)
                except pydantic.ValidationError:
                    pass
                else:
                    aggregate_snapshot = self._add_aggregate_snapshot(aggregate_snapshot, snapshot)

        if not all(complete_flags) or count < 7:
            aggregate_snapshot.complete = False
        return aggregate_snapshot
