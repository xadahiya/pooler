import asyncio
import json
import multiprocessing
import resource
from signal import SIGINT, SIGQUIT, SIGTERM
import time
from functools import partial
from typing import Dict
from typing import Union
from uuid import uuid4
from signal import signal
from aio_pika import IncomingMessage
from aio_pika import Message
from aio_pika.pool import Pool
from eth_utils import keccak
from httpx import AsyncClient
from httpx import AsyncHTTPTransport
from httpx import Limits
from httpx import Timeout
from pydantic import BaseModel
from redis import asyncio as aioredis
from web3 import Web3
from ipfs_client.main import AsyncIPFSClient
from tenacity import retry, stop_after_attempt, wait_random_exponential
import tenacity
from snapshotter.settings.config import settings
from snapshotter.utils.callback_helpers import get_rabbitmq_channel
from snapshotter.utils.callback_helpers import get_rabbitmq_robust_connection_async
from snapshotter.utils.data_utils import get_source_chain_id
from snapshotter.utils.default_logger import logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.models.data_models import SnapshotterIssue, SnapshotterReportState, SnapshotterStates, UnfinalizedSnapshot
from snapshotter.utils.models.data_models import SnapshotterStates
from snapshotter.utils.models.data_models import SnapshotterStateUpdate
from snapshotter.utils.models.message_models import AggregateBase
from snapshotter.utils.models.message_models import PayloadCommitMessage
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.models.message_models import PowerloomSnapshotSubmittedMessage
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import epoch_id_project_to_state_mapping, submitted_unfinalized_snapshot_cids
from snapshotter.utils.rpc import RpcHelper


class GenericAsyncWorker(multiprocessing.Process):
    _async_transport: AsyncHTTPTransport
    _rmq_connection_pool: Pool
    _rmq_channel_pool: Pool
    _aioredis_pool: RedisPoolCache
    _redis_conn: aioredis.Redis
    _rpc_helper: RpcHelper
    _anchor_rpc_helper: RpcHelper
    _httpx_client: AsyncClient
    _web3_storage_upload_transport: AsyncHTTPTransport
    _web3_storage_upload_client: AsyncClient

    def __init__(self, name, **kwargs):
        self._core_rmq_consumer: asyncio.Task
        self._exchange_name = f'{settings.rabbitmq.setup.callbacks.exchange}:{settings.namespace}'
        self._unique_id = f'{name}-' + keccak(text=str(uuid4())).hex()[:8]
        self._running_callback_tasks: Dict[str, asyncio.Task] = dict()
        super(GenericAsyncWorker, self).__init__(name=name, **kwargs)
        self._logger = logger.bind(module=self.name)
        self.protocol_state_contract = None
        self._qos = 20

        self._rate_limiting_lua_scripts = None

        self.protocol_state_contract_abi = read_json_file(
            settings.protocol_state.abi,
            self._logger,
        )
        self.protocol_state_contract_address = settings.protocol_state.address
        self._commit_payload_exchange = (
            f'{settings.rabbitmq.setup.commit_payload.exchange}:{settings.namespace}'
        )
        self._event_detector_exchange = f'{settings.rabbitmq.setup.event_detector.exchange}:{settings.namespace}'
        self._event_detector_routing_key_prefix = f'powerloom-event-detector:{settings.namespace}:{settings.instance_id}.'
        self._commit_payload_routing_key = (
            f'powerloom-backend-commit-payload:{settings.namespace}:{settings.instance_id}.Data'
        )
        self._initialized = False

    def _signal_handler(self, signum, frame):
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._core_rmq_consumer.cancel()

    def _web3_storage_retry_state_callback(self, retry_state: tenacity.RetryCallState):
        if retry_state and retry_state.outcome.failed:
            self._logger.warning(f'Encountered web3 storage upload exception: {retry_state.outcome.exception()} | args: {retry_state.args}, kwargs:{retry_state.kwargs}')

    @retry(
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(5),
    )
    async def _upload_web3_storage(self, snapshot: Union[BaseModel, AggregateBase]):
        web3_storage_settings = settings.web3storage
        await self._web3_storage_upload_client.post(
            url=f'{web3_storage_settings.url}{web3_storage_settings.upload_url_suffix}',
            json=snapshot.dict(by_alias=True),
        )
        self._logger.info('Uploaded snapshot to web3 storage: {}', snapshot)

    async def _commit_payload(
            self,
            type_: str,
            _ipfs_writer_client: AsyncIPFSClient,
            project_id: str,
            epoch: Union[
                PowerloomSnapshotProcessMessage,
                PowerloomSnapshotSubmittedMessage,
                PowerloomCalculateAggregateMessage,
            ],
            snapshot: Union[BaseModel, AggregateBase],
            storage_flag: bool,
    ):
        # payload commit sequence begins
        # upload to IPFS
        try:
            snapshot_cid = await _ipfs_writer_client.add_json(snapshot.dict(by_alias=True))
        except Exception as e:
            self._logger.opt(exception=True).error(
                'Exception uploading snapshot to IPFS for epoch {}: {}, Error: {},'
                'sending failure notifications', epoch, snapshot, e,
            )
            notification_message = SnapshotterIssue(
                instanceID=settings.instance_id,
                issueType=SnapshotterReportState.MISSED_SNAPSHOT.value,
                projectID=project_id,
                epochId=str(epoch.epochId),
                timeOfReporting=str(time.time()),
                extra=json.dumps({'issueDetails': f'Error : {e}'}),
            )
            await send_failure_notifications_async(
                client=self._client, message=notification_message,
            )
        else:
            # add to zset of unfinalized snapshot CIDs
            unfinalized_entry = UnfinalizedSnapshot(
                snapshotCid=snapshot_cid,
                snapshot=snapshot.dict(by_alias=True),
            )
            await self._redis_conn.zadd(
                name=submitted_unfinalized_snapshot_cids(project_id),
                mapping={unfinalized_entry.json(): epoch.epochId},
            )
            # publish snapshot submitted event to event detector queue
            snapshot_submitted_message = PowerloomSnapshotSubmittedMessage(
                snapshotCid=snapshot_cid,
                epochId=epoch.epochId,
                projectId=project_id,
                timestamp=int(time.time()),
            )
            try:
                async with self._rmq_connection_pool.acquire() as connection:
                    async with self._rmq_channel_pool.acquire() as channel:
                        # Prepare a message to send
                        commit_payload_exchange = await channel.get_exchange(
                            name=self._event_detector_exchange,
                        )
                        message_data = snapshot_submitted_message.json().encode()

                        # Prepare a message to send
                        message = Message(message_data)

                        await commit_payload_exchange.publish(
                            message=message,
                            routing_key=self._event_detector_routing_key_prefix + 'SnapshotSubmitted',
                        )

                        self._logger.debug(
                            'Sent snapshot submitted message to event detector queue | '
                            'Project: {} | Epoch: {} | Snapshot CID: {}',
                            project_id, epoch.epochId, snapshot_cid,
                        )

            except Exception as e:
                self._logger.opt(exception=True).error(
                    'Exception sending snapshot submitted message to event detector queue: {} | Project: {} | Epoch: {} | Snapshot CID: {}',
                    e, project_id, epoch.epochId, snapshot_cid,
                )

            try:
                await self._redis_conn.zremrangebyscore(
                    name=submitted_unfinalized_snapshot_cids(project_id),
                    min='-inf',
                    max=epoch.epochId - 32,
                )
            except:
                pass
            # send to relayer dispatch queue
            await self._send_payload_commit_service_queue(
                type_=type_,
                project_id=project_id,
                epoch=epoch,
                snapshot_cid=snapshot_cid,
            )

        # upload to web3 storage
        if storage_flag:
            asyncio.ensure_future(self._upload_web3_storage(snapshot))

    async def _rabbitmq_consumer(self, loop):
        self._rmq_connection_pool = Pool(get_rabbitmq_robust_connection_async, max_size=5, loop=loop)
        self._rmq_channel_pool = Pool(
            partial(get_rabbitmq_channel, self._rmq_connection_pool), max_size=20,
            loop=loop,
        )
        async with self._rmq_channel_pool.acquire() as channel:
            await channel.set_qos(self._qos)
            exchange = await channel.get_exchange(
                name=self._exchange_name,
            )
            q_obj = await channel.get_queue(
                name=self._q,
                ensure=False,
            )
            self._logger.debug(
                f'Consuming queue {self._q} with routing key {self._rmq_routing}...',
            )
            await q_obj.bind(exchange, routing_key=self._rmq_routing)
            await q_obj.consume(self._on_rabbitmq_message)

    async def _send_payload_commit_service_queue(
        self,
        type_: str,
        project_id: str,
        epoch: Union[
            PowerloomSnapshotProcessMessage,
            PowerloomSnapshotSubmittedMessage,
            PowerloomCalculateAggregateMessage,
        ],
        snapshot_cid: str,
    ):
        try:
            source_chain_details = await get_source_chain_id(
                redis_conn=self._redis_conn,
                rpc_helper=self._anchor_rpc_helper,
                state_contract_obj=self.protocol_state_contract,
            )
        except Exception as e:
            self._logger.opt(exception=True).error(
                'Exception getting source chain id: {}', e,
            )
            raise e
        commit_payload = PayloadCommitMessage(
            sourceChainId=source_chain_details,
            projectId=project_id,
            epochId=epoch.epochId,
            snapshotCID=snapshot_cid,
        )

        # send through rabbitmq
        try:
            async with self._rmq_connection_pool.acquire() as connection:
                async with self._rmq_channel_pool.acquire() as channel:
                    # Prepare a message to send
                    commit_payload_exchange = await channel.get_exchange(
                        name=self._commit_payload_exchange,
                    )
                    message_data = commit_payload.json().encode()

                    # Prepare a message to send
                    message = Message(message_data)

                    await commit_payload_exchange.publish(
                        message=message,
                        routing_key=self._commit_payload_routing_key,
                    )

                    self._logger.info(
                        'Sent message to commit payload queue: {}', commit_payload,
                    )

        except Exception as e:
            self._logger.opt(exception=True).error(
                (
                    'Exception committing snapshot CID {} to commit payload queue:'
                    ' {} | dump: {}'
                ),
                snapshot_cid,
                e,
            )
            await self._redis_conn.hset(
                name=epoch_id_project_to_state_mapping(
                    epoch.epochId, SnapshotterStates.SNAPSHOT_SUBMIT_PAYLOAD_COMMIT.value,
                ),
                mapping={
                    project_id: SnapshotterStateUpdate(
                        status='failed', error=str(e), timestamp=int(time.time()),
                    ).json(),
                },
            )
        else:
            await self._redis_conn.hset(
                name=epoch_id_project_to_state_mapping(
                    epoch.epochId, SnapshotterStates.SNAPSHOT_SUBMIT_PAYLOAD_COMMIT.value,
                ),
                mapping={
                    project_id: SnapshotterStateUpdate(
                        status='success', timestamp=int(time.time()),
                    ).json(),
                },
            )
    
    async def _on_rabbitmq_message(self, message: IncomingMessage):
        pass

    async def _init_redis_pool(self):
        self._aioredis_pool = RedisPoolCache()
        await self._aioredis_pool.populate()
        self._redis_conn = self._aioredis_pool._aioredis_pool

    async def _init_rpc_helper(self):
        self._rpc_helper = RpcHelper(rpc_settings=settings.rpc)
        self._anchor_rpc_helper = RpcHelper(rpc_settings=settings.anchor_chain_rpc)

        self.protocol_state_contract = self._anchor_rpc_helper.get_current_node()['web3_client'].eth.contract(
            address=Web3.toChecksumAddress(
                self.protocol_state_contract_address,
            ),
            abi=self.protocol_state_contract_abi,
        )
        # cleaning up ABI
        self.protocol_state_contract_abi = None

    async def _init_httpx_client(self):
        self._async_transport = AsyncHTTPTransport(
            limits=Limits(
                max_connections=200,
                max_keepalive_connections=50,
                keepalive_expiry=None,
            ),
        )
        self._client = AsyncClient(
            timeout=Timeout(timeout=5.0),
            follow_redirects=False,
            transport=self._async_transport,
        )
        self._web3_storage_upload_transport = AsyncHTTPTransport(
            limits=Limits(
                max_connections=200,
                max_keepalive_connections=settings.web3storage.max_idle_conns,
                keepalive_expiry=settings.web3storage.idle_conn_timeout,
            ),
        )
        self._web3_storage_upload_client = AsyncClient(
            timeout=Timeout(timeout=settings.web3storage.timeout),
            follow_redirects=False,
            transport=self._web3_storage_upload_transport,
        )

    async def init(self):
        if not self._initialized:
            await self._init_redis_pool()
            await self._init_httpx_client()
            await self._init_rpc_helper()
        self._initialized = True

    def run(self) -> None:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )
        for signame in [SIGINT, SIGTERM, SIGQUIT]:
            signal(signame, self._signal_handler)
        ev_loop = asyncio.get_event_loop()
        self._logger.debug(
            f'Starting asynchronous callback worker {self._unique_id}...',
        )
        self._core_rmq_consumer = asyncio.ensure_future(
            self._rabbitmq_consumer(ev_loop),
        )
        try:
            ev_loop.run_forever()
        finally:
            ev_loop.close()
