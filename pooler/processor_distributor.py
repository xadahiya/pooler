import asyncio
import json
import multiprocessing
import queue
import time
from functools import partial
from typing import Union
from uuid import uuid4

from aio_pika import IncomingMessage
from aio_pika import Message
from aio_pika.pool import Pool
from eth_utils import keccak
from pydantic import ValidationError
from setproctitle import setproctitle
from web3 import Web3

from pooler.settings.config import aggregator_config
from pooler.settings.config import projects_config
from pooler.settings.config import settings
from pooler.utils.callback_helpers import get_rabbitmq_channel
from pooler.utils.callback_helpers import get_rabbitmq_connection
from pooler.utils.data_utils import get_source_chain_epoch_size
from pooler.utils.data_utils import get_source_chain_id
from pooler.utils.default_logger import logger
from pooler.utils.models.message_models import EpochBroadcast
from pooler.utils.models.message_models import PayloadCommitFinalizedMessage
from pooler.utils.models.message_models import PowerloomCalculateAggregateMessage
from pooler.utils.models.message_models import PowerloomSnapshotFinalizedMessage
from pooler.utils.models.message_models import PowerloomSnapshotProcessMessage
from pooler.utils.models.message_models import PowerloomSnapshotSubmittedMessage
from pooler.utils.models.settings_model import AggregateOn
from pooler.utils.redis.redis_conn import RedisPoolCache
from pooler.utils.redis.redis_keys import project_finalized_data_zset
from pooler.utils.rpc import RpcHelper
from pooler.utils.snapshot_utils import warm_up_cache_for_snapshot_constructors


class ProcessorDistributor(multiprocessing.Process):
    def __init__(self, name, **kwargs):
        """
    Initializes a ProcessorDistributor object.

    Args:
        name (str): The name of the ProcessorDistributor.
        **kwargs: Additional keyword arguments.

    Attributes:
        _unique_id (str): The unique ID of the ProcessorDistributor.
        _q (queue.Queue): The queue used for processing.
        _rabbitmq_interactor: The RabbitMQ interactor object.
        _shutdown_initiated (bool): Flag indicating if shutdown has been initiated.
        _redis_conn: The Redis connection object.
        _aioredis_pool: The aioredis pool object.
        _rpc_helper: The RPC helper object.
        _source_chain_id: The source chain ID.
        _consume_exchange_name (str): The name of the exchange used for consuming events.
        _consume_queue_name (str): The name of the queue used for consuming events.
        _initialized (bool): Flag indicating if the ProcessorDistributor has been initialized.
        _consume_queue_routing_key (str): The routing key for consuming events.
        _callback_exchange_name (str): The name of the exchange used for callbacks.
        _payload_commit_exchange_name (str): The name of the exchange used for payload commits.
        _payload_commit_routing_key (str): The routing key for payload commits.


    """
        super(ProcessorDistributor, self).__init__(name=name, **kwargs)
        self._unique_id = f'{name}-' + keccak(text=str(uuid4())).hex()[:8]
        self._q = queue.Queue()
        self._rabbitmq_interactor = None
        self._shutdown_initiated = False
        self._redis_conn = None
        self._aioredis_pool = None
        self._rpc_helper = None
        self._source_chain_id = None
        self._consume_exchange_name = f'{settings.rabbitmq.setup.event_detector.exchange}:{settings.namespace}'
        self._consume_queue_name = (
            f'powerloom-event-detector:{settings.namespace}:{settings.instance_id}'
        )
        self._initialized = False
        self._consume_queue_routing_key = f'powerloom-event-detector:{settings.namespace}:{settings.instance_id}.*'
        self._callback_exchange_name = (
            f'{settings.rabbitmq.setup.callbacks.exchange}:{settings.namespace}'
        )
        self._payload_commit_exchange_name = (
            f'{settings.rabbitmq.setup.commit_payload.exchange}:{settings.namespace}'
        )
        self._payload_commit_routing_key = f'powerloom-backend-commit-payload:{settings.namespace}:{settings.instance_id}.Finalized'

    async def _init_redis_pool(self):
        """
    Initialize the Redis connection pool.

    This function initializes the Redis connection pool if it has not been
    initialized already. It creates an instance of the RedisPoolCache class
    and populates it with the necessary Redis connections. The Redis connection
    pool is then assigned to the `_redis_conn` attribute for future use.

    Returns:
        None


    """
        if not self._aioredis_pool:
            self._aioredis_pool = RedisPoolCache()
            await self._aioredis_pool.populate()
            self._redis_conn = self._aioredis_pool._aioredis_pool

    async def _init_rpc_helper(self):
        """
    Initializes the RPC helper for the current instance. If the RPC helper is not already set, it creates a new instance of RpcHelper. It also creates a new instance of RpcHelper specifically for the anchor chain, using the rpc_settings from the settings module.

    It then loads the ABI (Application Binary Interface) from the protocol_state.abi file in the settings module. The ABI is a JSON object that defines the interface of a smart contract.

    Next, it creates a contract object for the protocol state contract on the anchor chain using the address and ABI. It uses the web3_client from the current node in the anchor RPC helper.

    It then calls the get_source_chain_epoch_size function, passing the Redis connection, anchor RPC helper, and protocol state contract object as arguments. This function retrieves the epoch size of the source chain and stores it in Redis.

    After that, it calls the get_source_chain_id function, passing the Redis connection, anchor RPC helper, and protocol state contract object as arguments. This function retrieves the ID of the source chain and stores it in Redis.

    Finally, it sets the source chain ID for the current instance to the value returned by get_source_chain_id.
    """
        if not self._rpc_helper:
            self._rpc_helper = RpcHelper()
            self._anchor_rpc_helper = RpcHelper(rpc_settings=settings.anchor_chain_rpc)
            with open(settings.protocol_state.abi, 'r') as f:
                abi_dict = json.load(f)
            protocol_state_contract = self._anchor_rpc_helper.get_current_node()['web3_client'].eth.contract(
                address=Web3.toChecksumAddress(
                    settings.protocol_state.address,
                ),
                abi=abi_dict,
            )
            await get_source_chain_epoch_size(
                redis_conn=self._redis_conn,
                rpc_helper=self._anchor_rpc_helper,
                state_contract_obj=protocol_state_contract,
            )
            self._source_chain_id = await get_source_chain_id(
                redis_conn=self._redis_conn,
                rpc_helper=self._anchor_rpc_helper,
                state_contract_obj=protocol_state_contract,
            )

    async def _warm_up_cache_for_epoch_data(
        self, msg_obj: PowerloomSnapshotProcessMessage,
    ):
        """

    Warms up the cache for epoch data by calling the `warm_up_cache_for_snapshot_constructors` function with the specified block range. This function is intended to be used as a handler for a PowerloomSnapshotProcessMessage.

    Args:
        msg_obj (PowerloomSnapshotProcessMessage): The message object containing the block range.

    Raises:
        Exception: If there is an error while warming up the cache.

    Returns:
        None

    """

        try:
            max_chain_height = msg_obj.end
            min_chain_height = msg_obj.begin
            await warm_up_cache_for_snapshot_constructors(
                from_block=min_chain_height,
                to_block=max_chain_height,
                redis_conn=self._redis_conn,
                rpc_helper=self._rpc_helper,
            )

    except Exception as exc:
        self._logger.warning(
            (
                'There was an error while warming-up cache for epoch data.'
                f' error_msg: {exc}'
            ),
        )

    async def _publish_message_to_queue(
        self,
        exchange,
        routing_key,
        message: Union[
            EpochBroadcast,
            PowerloomSnapshotSubmittedMessage,
            PowerloomCalculateAggregateMessage,
            PowerloomSnapshotFinalizedMessage,
        ],
    ):
        """
    Sends a message to a queue in RabbitMQ.

    Args:
        exchange (str): The name of the exchange to send the message to.
        routing_key (str): The routing key for the message.
        message (Union[EpochBroadcast, PowerloomSnapshotSubmittedMessage, PowerloomCalculateAggregateMessage, PowerloomSnapshotFinalizedMessage]): The message to send to the queue.

    Raises:
        Exception: If there is an error sending the message to the queue.

    Returns:
        None.

    Note:
        This function acquires a connection and channel from the connection and channel pools respectively. It then prepares the message to send by encoding it as JSON. The message is then published to the exchange with the specified routing key. If successful, the function logs the message sent to the queue. If an exception occurs, it logs the exception along with the message and dumps the exception details.
    """
        try:
            async with self._rmq_connection_pool.acquire() as connection:
                async with self._rmq_channel_pool.acquire() as channel:
                    # Prepare a message to send
                    exchange = await channel.get_exchange(
                        name=exchange,
                    )
                    message_data = message.json().encode()

                    # Prepare a message to send
                    queue_message = Message(message_data)

                    await exchange.publish(
                        message=queue_message,
                        routing_key=routing_key,
                    )

                    self._logger.info(
                        'Sent message to queue: {}', message,
                    )

        except Exception as e:
            self._logger.opt(exception=True).error(
                (
                    'Exception sending message to queue. '
                    ' {} | dump: {}'
                ),
                message,
                e,
            )

    async def _distribute_callbacks_snapshotting(self, message: IncomingMessage):
        """

    Distribute callbacks for snapshotting.

    Args:
        message (IncomingMessage): The incoming message containing the epoch broadcast.

    Returns:
        None

    Raises:
        ValidationError: If the message structure of the epoch callback is invalid.
        Exception: If the message format of the epoch callback is unexpected.

    Notes:
        This function is responsible for distributing callbacks for snapshotting. It first parses the incoming message into an EpochBroadcast object. If the message structure is invalid, it logs an error and returns. If the message format is unexpected, it logs an error and returns.

        The function then warms up the cache for epoch data by calling the _warm_up_cache_for_epoch_data method asynchronously.

        Next, it iterates over the project configurations and projects to construct PowerloomSnapshotProcessMessage objects for each project. These objects contain the necessary information for processing the snapshots. The function publishes these messages to a queue for further processing by a worker.

        After all the messages have been published, the function waits for the tasks to complete using asyncio.gather. It collects the results and logs any errors that occurred while sending messages to the queue.

        This function is intended to be used as a route handler in a web application.

    """
        try:
            msg_obj: EpochBroadcast = (
                EpochBroadcast.parse_raw(message.body)
            )
        except ValidationError:
            self._logger.opt(exception=True).error(
                'Bad message structure of epoch callback',
            )
            return
        except Exception:
            self._logger.opt(exception=True).error(
                'Unexpected message format of epoch callback',
            )
            return
        self._logger.debug(f'Epoch Distribution time - {int(time.time())}')
        # warm-up cache before constructing snapshots
        await self._warm_up_cache_for_epoch_data(msg_obj=msg_obj)

        queuing_tasks = []

        for project_config in projects_config:
            type_ = project_config.project_type
            for project in project_config.projects:
                contract = project.lower()
                process_unit = PowerloomSnapshotProcessMessage(
                    begin=msg_obj.begin,
                    end=msg_obj.end,
                    epochId=msg_obj.epochId,
                    contract=contract,
                    broadcastId=msg_obj.broadcastId,
                )

                queuing_tasks.append(
                    self._publish_message_to_queue(
                        exchange=self._callback_exchange_name,
                        routing_key=f'powerloom-backend-callback:{settings.namespace}:{settings.instance_id}:EpochReleased.{type_}',
                        message=process_unit,
                    ),
                )

                self._logger.debug(
                    'Sent out message to be processed by worker'
                    f' {type_} : {process_unit}',
                )

        results = await asyncio.gather(*queuing_tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                self._logger.error(
                    'Error while sending message to queue. Error - {}',
                    result,
                )

    async def _cache_and_forward_to_payload_commit_queue(self, message: IncomingMessage):
        """
    This function is responsible for caching and forwarding a message to the payload commit queue. It takes in an incoming message as a parameter. The function first extracts the event type from the routing key of the message. If the event type is "SnapshotFinalized", it parses the message body into a PowerloomSnapshotFinalizedMessage object. Otherwise, it returns without further processing.

    Next, the function adds the snapshot CID and epoch ID to a sorted set in Redis using the project ID as the key. This is done using the zadd method of the Redis connection.

    The function then logs the distribution time of the payload commit message using the logger.

    After that, it creates a PayloadCommitFinalizedMessage object with the parsed message, sets the web3Storage flag to True, and sets the sourceChainId to the value of the _source_chain_id attribute.

    Finally, the function publishes the process_unit message to the payload commit queue by calling the _publish_message_to_queue method with the appropriate exchange, routing key, and message.

    The function ends by logging a debug message indicating that the event has been sent to the payload commit queue.

    Note: This function is an asynchronous function and should be awaited when called.
    """
        event_type = message.routing_key.split('.')[-1]

        if event_type == 'SnapshotFinalized':
            msg_obj: PowerloomSnapshotFinalizedMessage = (
                PowerloomSnapshotFinalizedMessage.parse_raw(message.body)
            )
        else:
            return

        # Add to project finalized data zset
        await self._redis_conn.zadd(
            project_finalized_data_zset(project_id=msg_obj.projectId),
            {msg_obj.snapshotCid: msg_obj.epochId},
        )

        self._logger.debug(f'Payload Commit Message Distribution time - {int(time.time())}')

        process_unit = PayloadCommitFinalizedMessage(
            message=msg_obj,
            web3Storage=True,
            sourceChainId=self._source_chain_id,
        )

        await self._publish_message_to_queue(
            exchange=self._payload_commit_exchange_name,
            routing_key=self._payload_commit_routing_key,
            message=process_unit,
        )

        self._logger.debug(
            (
                'Sent out Event to Payload Commit Queue'
                f' {event_type} : {process_unit}'
            ),
        )

    async def _distribute_callbacks_aggregate(self, message: IncomingMessage):
        """
    This function is responsible for distributing and aggregating callbacks received from a message queue. It takes in an incoming message and processes it based on its event type. If the event type is not recognized, an error is logged and the function returns. If the message structure is invalid, an error is logged and the function returns.

    The function then proceeds to distribute the callback based on the aggregator configuration. It iterates through each configuration and checks if the message matches the criteria for aggregation. If the aggregation is based on a single project, it checks if the project ID of the message matches the configured project ID. If not, it logs a message and continues to the next configuration. If the aggregation is based on multiple projects, it checks if the project ID of the message is in the list of projects to wait for. If not, it logs a message and continues to the next configuration.

    For configurations that match, the function performs the necessary aggregation steps. It cleans up previous epochs in Redis, adds the current message to the Redis sorted set, retrieves all events for the current epoch, checks if all required projects are present, and if so, aggregates the messages and publishes the final message to the message queue. Finally, it cleans up the Redis sorted set for the current epoch.

    The function logs various informational messages throughout the process to provide visibility into the aggregation progress.

    Note: This function assumes the existence of certain variables and dependencies, such as `aggregator_config`, `PowerloomSnapshotSubmittedMessage`, `PowerloomCalculateAggregateMessage`, `settings`, `time`, `uuid4`, and `_publish_message_to_queue`. Make sure these are properly defined and imported before using this function.
    """
        event_type = message.routing_key.split('.')[-1]
        try:
            if event_type != 'SnapshotSubmitted':
                self._logger.error(f'Unknown event type {event_type}')
                return

            process_unit: PowerloomSnapshotSubmittedMessage = (
                PowerloomSnapshotSubmittedMessage.parse_raw(message.body)
            )

        except ValidationError:
            self._logger.opt(exception=True).error(
                'Bad message structure of event callback',
            )
            return
        except Exception:
            self._logger.opt(exception=True).error(
                'Unexpected message format of event callback',
            )
            return
        self._logger.debug(f'Aggregation Task Distribution time - {int(time.time())}')

        # go through aggregator config, if it matches then send appropriate message

        for config in aggregator_config:
            type_ = config.project_type

            if config.aggregate_on == AggregateOn.single_project:
                if config.filters.projectId not in process_unit.projectId:
                    self._logger.info(f'projectId mismatch {process_unit.projectId} {config.filters.projectId}')
                    continue

                await self._publish_message_to_queue(
                    exchange=self._callback_exchange_name,
                    routing_key=f'powerloom-backend-callback:{settings.namespace}:'
                    f'{settings.instance_id}:CalculateAggregate.{type_}',
                    message=process_unit,
                )
            elif config.aggregate_on == AggregateOn.multi_project:
                if process_unit.projectId not in config.projects_to_wait_for:
                    self._logger.info(f'projectId not required for {config.project_type}: {process_unit.projectId}')
                    continue

                # cleanup redis for all previous epochs (5 buffer)
                await self._redis_conn.zremrangebyscore(
                    f'powerloom:aggregator:{config.project_type}:events',
                    0,
                    process_unit.epochId - 5,
                )

                await self._redis_conn.zadd(
                    f'powerloom:aggregator:{config.project_type}:events',
                    {process_unit.json(): process_unit.epochId},
                )

                events = await self._redis_conn.zrangebyscore(
                    f'powerloom:aggregator:{config.project_type}:events',
                    process_unit.epochId,
                    process_unit.epochId,
                )

                if not events:
                    self._logger.info(f'No events found for {process_unit.epochId}')
                    continue

                event_project_ids = set()
                finalized_messages = list()

                for event in events:
                    event = PowerloomSnapshotSubmittedMessage.parse_raw(event)
                    event_project_ids.add(event.projectId)
                    finalized_messages.append(event)

                if event_project_ids == set(config.projects_to_wait_for):
                    self._logger.info(f'All projects present for {process_unit.epochId}, aggregating')
                    final_msg = PowerloomCalculateAggregateMessage(
                        messages=finalized_messages,
                        epochId=process_unit.epochId,
                        timestamp=int(time.time()),
                        broadcastId=str(uuid4()),
                    )

                    await self._publish_message_to_queue(
                        exchange=self._callback_exchange_name,
                        routing_key=f'powerloom-backend-callback:{settings.namespace}'
                        f':{settings.instance_id}:CalculateAggregate.{type_}',
                        message=final_msg,
                    )

                    # Cleanup redis for current epoch

                    await self._redis_conn.zremrangebyscore(
                        f'powerloom:aggregator:{config.project_type}:events',
                        process_unit.epochId,
                        process_unit.epochId,
                    )

                else:
                    self._logger.info(
                        f'Not all projects present for {process_unit.epochId},'
                        f' {len(set(config.projects_to_wait_for)) - len(event_project_ids)} missing',
                    )

    async def _on_rabbitmq_message(self, message: IncomingMessage):
        """
    This function is an asynchronous method that handles incoming messages from RabbitMQ. It takes in a message object as a parameter and acknowledges the message.

    The function first extracts the message type from the routing key of the message. It then logs a debug message indicating that a message has been received for processing and distribution.

    If the function has not been initialized yet, it initializes the Redis pool and RPC helper. The initialization is done only once to avoid redundant setup.

    Based on the message type, the function performs different actions. If the message type is "EpochReleased", it calls the `_distribute_callbacks_snapshotting` method to distribute callbacks for snapshotting. If the message type is "SnapshotSubmitted", it calls the `_distribute_callbacks_aggregate` method to distribute callbacks for aggregation. If the message type is "SnapshotFinalized", it calls the `_cache_and_forward_to_payload_commit_queue` method to cache and forward the message to the payload commit queue.

    If the message type is unknown, the function logs an error message indicating the unknown routing key.

    Note: This function is likely part of a larger system or application and may have dependencies on other methods or classes. Please refer to the complete codebase for a better understanding of its usage and functionality.
    """
        await message.ack()

        message_type = message.routing_key.split('.')[-1]
        self._logger.debug(
            (
                'Got message to process and distribute: {}'
            ),
            message.body,
        )

        if not self._initialized:
            await self._init_redis_pool()
            await self._init_rpc_helper()
            self._initialized = True

        if message_type == 'EpochReleased':
            await self._distribute_callbacks_snapshotting(
                message,
            )

        elif message_type == 'SnapshotSubmitted':
            await self._distribute_callbacks_aggregate(
                message,
            )

        elif message_type == 'SnapshotFinalized':
            await self._cache_and_forward_to_payload_commit_queue(
                message,
            )

        else:
            self._logger.error(
                (
                    'Unknown routing key for callback distribution: {}'
                ),
                message.routing_key,
            )

    async def _rabbitmq_consumer(self, loop):
        """

    Asynchronous function that handles consuming messages from a RabbitMQ queue.

    Args:
        self: The instance of the class that this function belongs to.
        loop: The event loop to use for the asynchronous operations.

    Returns:
        None

    Raises:
        None

    This function establishes a connection pool and a channel pool for RabbitMQ. It acquires a channel from the channel pool and sets the quality of service (QoS) to 100.
    It then gets the exchange and queue objects from the channel. The function binds the queue to the exchange with the specified routing key. Finally, it starts consuming
    messages from the queue and passes each message to the _on_rabbitmq_message callback function.

    Note:
        This function is intended to be used as a consumer for RabbitMQ messages in an asynchronous environment.

    """
        self._rmq_connection_pool = Pool(get_rabbitmq_connection, max_size=5, loop=loop)
        self._rmq_channel_pool = Pool(
            partial(get_rabbitmq_channel, self._rmq_connection_pool), max_size=20,
            loop=loop,
        )
        async with self._rmq_channel_pool.acquire() as channel:
            await channel.set_qos(100)
            exchange = await channel.get_exchange(
                name=self._consume_exchange_name,
            )
            q_obj = await channel.get_queue(
                name=self._consume_queue_name,
                ensure=False,
            )
            self._logger.debug(
                f'Consuming queue {self._consume_queue_name} with routing key {self._consume_queue_routing_key}...',
            )
            await q_obj.bind(exchange, routing_key=self._consume_queue_routing_key)
            await q_obj.consume(self._on_rabbitmq_message)

    def run(self) -> None:
        """

    Runs the process distributor.

    Sets the process title to the name of the process.

    Binds the logger to the module name.

    Gets the event loop.

    Starts the RabbitMQ consumer on the specified queue for the Processor Distributor.

    Runs the event loop indefinitely.

    Closes the event loop when finished.

    """
        setproctitle(self.name)

        self._logger = logger.bind(
            module=f'PowerLoom|Callbacks|ProcessDistributor:{settings.namespace}-{settings.instance_id}',
        )

        ev_loop = asyncio.get_event_loop()
        self._logger.debug('Starting RabbitMQ consumer on queue {} for Processor Distributor', self._consume_queue_name)
        self._core_rmq_consumer = asyncio.ensure_future(
            self._rabbitmq_consumer(ev_loop),
        )
        try:
            ev_loop.run_forever()


finally:
    ev_loop.close()
