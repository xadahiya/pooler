import asyncio
import hashlib
import importlib
from typing import Callable
from typing import List
from typing import Union
from uuid import uuid4

from aio_pika import IncomingMessage
from aio_pika import Message
from ipfs_client.main import AsyncIPFSClient
from ipfs_client.main import AsyncIPFSClientSingleton
from pydantic import ValidationError

from pooler.settings.config import aggregator_config
from pooler.settings.config import projects_config
from pooler.settings.config import settings
from pooler.utils.callback_helpers import notify_on_task_failure_aggregate
from pooler.utils.data_utils import get_source_chain_id
from pooler.utils.generic_worker import GenericAsyncWorker
from pooler.utils.models.message_models import AggregateBase
from pooler.utils.models.message_models import PayloadCommitMessage
from pooler.utils.models.message_models import PowerloomCalculateAggregateMessage
from pooler.utils.models.message_models import PowerloomSnapshotSubmittedMessage
from pooler.utils.models.settings_model import AggregateOn
from pooler.utils.redis.rate_limiter import load_rate_limiter_scripts


class AggregationAsyncWorker(GenericAsyncWorker):
    _ipfs_singleton: AsyncIPFSClientSingleton
    _ipfs_writer_client: AsyncIPFSClient
    _ipfs_reader_client: AsyncIPFSClient

    def __init__(self, name, **kwargs):
        """
    This function is the constructor for the class `AggregationAsyncWorker`. It initializes various attributes and sets up the configuration for aggregation.

    Parameters:
    - `name` (str): The name of the worker.
    - `**kwargs` (dict): Additional keyword arguments.

    Attributes:
    - `_q` (str): The name of the RabbitMQ queue for backend aggregation.
    - `_rmq_routing` (str): The RabbitMQ routing key for backend callbacks.
    - `_project_calculation_mapping` (None): Mapping of project types to calculations.
    - `_initialized` (bool): Flag indicating if the worker has been initialized.
    - `_single_project_types` (set): Set of project types for single project aggregation.
    - `_multi_project_types` (set): Set of project types for multi-project aggregation.
    - `_task_types` (set): Set of all project types.
    - `_ipfs_singleton` (None): Singleton instance of IPFS.

    Configuration:
    - The function iterates over the `aggregator_config` list and sets up the configuration for aggregation based on the `aggregate_on` attribute of each config object. If `aggregate_on` is `AggregateOn.single_project`, the `project_type` is added to the `_single_project_types` set. If `aggregate_on` is `AggregateOn.multi_project`, the `project_type` is added to the `_multi_project_types` set. The `project_type` is also added to the `_task_types` set.

    Note:
    - This function should be called when creating an instance of `AggregationAsyncWorker`.
    """
        self._q = f'powerloom-backend-cb-aggregate:{settings.namespace}:{settings.instance_id}'
        self._rmq_routing = f'powerloom-backend-callback:{settings.namespace}'
        f':{settings.instance_id}:CalculateAggregate.*'
        super(AggregationAsyncWorker, self).__init__(name=name, **kwargs)

        self._project_calculation_mapping = None
        self._initialized = False
        self._single_project_types = set()
        self._multi_project_types = set()
        self._task_types = set()
        self._ipfs_singleton = None

        for config in aggregator_config:
            if config.aggregate_on == AggregateOn.single_project:
                self._single_project_types.add(config.project_type)
            elif config.aggregate_on == AggregateOn.multi_project:
                self._multi_project_types.add(config.project_type)
            self._task_types.add(config.project_type)

    @notify_on_task_failure_aggregate
    async def _processor_task(
        self,
        msg_obj: Union[PowerloomSnapshotSubmittedMessage, PowerloomCalculateAggregateMessage],
        task_type: str,
    ):
        """Function used to process the received message object."""
        self._logger.debug(
            'Processing callback: {}', msg_obj,
        )

        if task_type not in self._project_calculation_mapping:
            self._logger.error(
                (
                    'No project calculation mapping found for task type'
                    f' {task_type}. Skipping...'
                ),
            )
            return

        self_unique_id = str(uuid4())
        cur_task: asyncio.Task = asyncio.current_task(
            asyncio.get_running_loop(),
        )
        cur_task.set_name(
            f'aio_pika.consumer|Processor|{task_type}|{msg_obj.broadcastId}',
        )
        self._running_callback_tasks[self_unique_id] = cur_task

        try:
            if not self._rate_limiting_lua_scripts:
                self._rate_limiting_lua_scripts = await load_rate_limiter_scripts(
                    self._redis_conn,
                )
            self._logger.debug(
                'Got epoch to process for {}: {}',
                task_type, msg_obj,
            )

            stream_processor = self._project_calculation_mapping[task_type]

            snapshot = await self._map_processed_epochs_to_adapters(
                msg_obj=msg_obj,
                cb_fn_async=stream_processor.compute,
                task_type=task_type,
                transformation_lambdas=stream_processor.transformation_lambdas,
            )

            await self._send_payload_commit_service_queue(
                audit_stream=task_type,
                epoch=msg_obj,
                snapshot=snapshot,
            )
        except Exception as e:
            del self._running_callback_tasks[self_unique_id]
            raise e

    def _gen_single_type_project_id(self, type_, epoch):
        """
    Generates a project ID for a single type of project.

    Args:
        type_ (str): The type of the project.
        epoch (Epoch): The epoch object containing the project ID.

    Returns:
        str: The generated project ID.
    """
        contract = epoch.projectId.split(':')[-2]
        project_id = f'{type_}:{contract}:{settings.namespace}'
        return project_id

    def _gen_multiple_type_project_id(self, type_, epoch):
        """
    Generates a unique project ID based on the given type and epoch.

    Args:
        type_ (str): The type of the project.
        epoch (Epoch): The epoch object containing the project messages.

    Returns:
        str: The generated project ID.

    Example:
        ```
        type_ = "example"
        epoch = Epoch(messages=[project1, project2, project3])
        project_id = _gen_multiple_type_project_id(type_, epoch)
        ```
    """

        underlying_project_ids = [project.projectId for project in epoch.messages]
        unique_project_id = ''.join(sorted(underlying_project_ids))

        project_hash = hashlib.sha3_256(unique_project_id.encode()).hexdigest()

        project_id = f'{type_}:{project_hash}:{settings.namespace}'
        return project_id

    def _gen_project_id(self, type_, epoch):
        """
    Generates a project ID based on the given type and epoch.

    Args:
        type_ (str): The type of the project.
        epoch (int): The epoch of the project.

    Returns:
        str: The generated project ID.

    Raises:
        ValueError: If the given project type is unknown.
    """
        if type_ in self._single_project_types:
            return self._gen_single_type_project_id(type_, epoch)
        elif type_ in self._multi_project_types:
            return self._gen_multiple_type_project_id(type_, epoch)
        else:
            raise ValueError(f'Unknown project type {type_}')

    async def _send_payload_commit_service_queue(
        self,
        audit_stream,
        epoch: Union[PowerloomSnapshotSubmittedMessage, PowerloomCalculateAggregateMessage],
        snapshot: Union[AggregateBase, None],
    ):
        """
    Sends the payload of a commit to the service queue for the audit protocol.

    Args:
        audit_stream: The audit stream associated with the commit.
        epoch: The epoch for which the commit is being made. Can be either a PowerloomSnapshotSubmittedMessage or a PowerloomCalculateAggregateMessage.
        snapshot: The aggregate snapshot to be committed. Can be None if no snapshot is available.

    Raises:
        None

    Returns:
        None

    Notes:
        - If no snapshot is available, an error message is logged.
        - The source chain details are obtained using the get_source_chain_id function.
        - The payload is converted to a dictionary using the snapshot's dict() method.
        - The project ID is generated using the _gen_project_id method.
        - The commit payload message is created using the PayloadCommitMessage class.
        - The message is sent through RabbitMQ to the commit payload queue.
        - If an exception occurs during the process, an error message is logged.
    """

        if not snapshot:
            self._logger.error(
                (
                    'No aggreagate snapshot to commit. Construction of snapshot'
                    ' failed for {} against epoch {}'
                ),
                audit_stream,
                epoch,
            )

        else:

            source_chain_details = await get_source_chain_id(
                redis_conn=self._redis_conn,
                rpc_helper=self._anchor_rpc_helper,
                state_contract_obj=self.protocol_state_contract,
            )

            payload = snapshot.dict()

            project_id = self._gen_project_id(audit_stream, epoch)

            commit_payload = PayloadCommitMessage(
                message=payload,
                web3Storage=settings.web3storage.upload_aggregates,
                sourceChainId=source_chain_details,
                projectId=project_id,
                epochId=epoch.epochId,
            )

            exchange = (
                f'{settings.rabbitmq.setup.commit_payload.exchange}:{settings.namespace}'
            )
            routing_key = f'powerloom-backend-commit-payload:{settings.namespace}:{settings.instance_id}.Data'

            # send through rabbitmq
            try:
                async with self._rmq_connection_pool.acquire() as connection:
                    async with self._rmq_channel_pool.acquire() as channel:
                        # Prepare a message to send

                        commit_payload_exchange = await channel.get_exchange(
                            name=exchange,
                        )
                        message_data = commit_payload.json().encode()

                        # Prepare a message to send
                        message = Message(message_data)

                        await commit_payload_exchange.publish(
                            message=message,
                            routing_key=routing_key,
                        )

                        self._logger.info(
                            'Sent message to commit payload queue: {}', commit_payload,
                        )

            except Exception as e:
                self._logger.opt(exception=True).error(
                    (
                        'Exception committing snapshot to audit protocol:'
                        ' {} | dump: {}'
                    ),
                    snapshot,
                    e,
                )

    async def _map_processed_epochs_to_adapters(
        self,
        msg_obj: Union[PowerloomSnapshotSubmittedMessage, PowerloomCalculateAggregateMessage],
        cb_fn_async,
        task_type,
        transformation_lambdas: List[Callable],
    ):
        """
    This function maps processed epochs to adapters based on the given parameters. It takes in a message object, a callback function, a task type, and a list of transformation lambdas. It generates a project ID based on the task type and message object. Then, it calls the callback function asynchronously with the necessary parameters and stores the result. If there are transformation lambdas provided, it applies each lambda to the result. Finally, it returns the result.

    If any exception occurs during the process, it logs an error message and raises the exception.

    Parameters:
    - `msg_obj` (Union[PowerloomSnapshotSubmittedMessage, PowerloomCalculateAggregateMessage]): The message object containing information about the snapshot or aggregate calculation.
    - `cb_fn_async` (Callable): The callback function to be called asynchronously.
    - `task_type` (Any): The type of task being processed.
    - `transformation_lambdas` (List[Callable]): A list of transformation lambdas to be applied to the result.

    Returns:
    - The result of the callback function and the applied transformation lambdas.

    Raises:
    - Any exception that occurs during the process.
    """

        try:

            project_id = self._gen_project_id(task_type, msg_obj)

            result = await cb_fn_async(
                msg_obj=msg_obj,
                redis=self._redis_conn,
                rpc_helper=self._rpc_helper,
                anchor_rpc_helper=self._anchor_rpc_helper,
                ipfs_reader=self._ipfs_reader_client,
                protocol_state_contract=self.protocol_state_contract,
                project_id=project_id,
            )

            if transformation_lambdas:
                for each_lambda in transformation_lambdas:
                    result = each_lambda(result, msg_obj)

            return result

    except Exception as e:
        self._logger.opt(exception=True).error(
            (
                'Error while processing aggregate {} for callback processor'
                ' of type {}'
            ),
            msg_obj,
            task_type,
        )
        raise e

    async def _on_rabbitmq_message(self, message: IncomingMessage):
        """

    Handles incoming RabbitMQ messages and processes them based on their task type.

    Args:
        message (IncomingMessage): The incoming RabbitMQ message.

    Returns:
        None

    Raises:
        None

    Notes:
        - This function is an asynchronous function.
        - The task type is extracted from the routing key of the message.
        - If the task type is not supported, the function returns without processing the message.
        - The message is acknowledged to RabbitMQ.
        - The function initializes necessary resources before processing the message.
        - If the task type is a single project type, the message body is parsed into a PowerloomSnapshotSubmittedMessage object.
        - If the task type is a multi project type, the message body is parsed into a PowerloomCalculateAggregateMessage object.
        - If the message structure is invalid, an error is logged and the function returns.
        - If the task type is unknown, an error is logged and the function returns.
        - The processed message is passed to the _processor_task function for further processing.


    """
        task_type = message.routing_key.split('.')[-1]
        if task_type not in self._task_types:
            return

        await message.ack()

        await self.init()

        self._logger.debug('task type: {}', task_type)
        # TODO: Update based on new single project based design
        if task_type in self._single_project_types:
            try:
                msg_obj: PowerloomSnapshotSubmittedMessage = (
                    PowerloomSnapshotSubmittedMessage.parse_raw(message.body)
                )
            except ValidationError as e:
                self._logger.opt(exception=True).error(
                    (
                        'Bad message structure of callback processor. Error: {}'
                    ),
                    e,
                )
                return
            except Exception as e:
                self._logger.opt(exception=True).error(
                    (
                        'Unexpected message structure of callback in processor. Error: {}'
                    ),
                    e,
                )
                return
        elif task_type in self._multi_project_types:
            try:
                msg_obj: PowerloomCalculateAggregateMessage = (
                    PowerloomCalculateAggregateMessage.parse_raw(message.body)
                )
            except ValidationError as e:
                self._logger.opt(exception=True).error(
                    (
                        'Bad message structure of callback processor. Error: {}'
                    ),
                    e,
                )
                return
            except Exception as e:
                self._logger.opt(exception=True).error(
                    (
                        'Unexpected message structure of callback in processor. Error: {}'
                    ),
                    e,
                )
                return
        else:
            self._logger.error(
                'Unknown task type {}', task_type,
            )
            return
        asyncio.ensure_future(self._processor_task(msg_obj=msg_obj, task_type=task_type))

    async def _init_project_calculation_mapping(self):
        """
    Initializes the project calculation mapping for the current instance. If the mapping is already initialized, the function returns without making any changes. Otherwise, it creates an empty dictionary to store the mapping.

    The function then iterates over the aggregator_config list and retrieves the project type from each project_config object. It checks if the project type already exists in the mapping dictionary. If it does, an exception is raised indicating a duplicate project type. Otherwise, it imports the module specified in the project_config object and retrieves the class specified by the class_name attribute. It then adds an entry to the mapping dictionary with the project type as the key and an instance of the retrieved class as the value.

    Next, the function iterates over the projects_config list and performs the same steps as above to add entries to the mapping dictionary.

    Note: This function assumes that the aggregator_config and projects_config lists are defined and accessible within the scope of the function.
    """
        if self._project_calculation_mapping is not None:
            return

        self._project_calculation_mapping = dict()
        for project_config in aggregator_config:
            key = project_config.project_type
            if key in self._project_calculation_mapping:
                raise Exception('Duplicate project type found')
            module = importlib.import_module(project_config.processor.module)
            class_ = getattr(module, project_config.processor.class_name)
            self._project_calculation_mapping[key] = class_()
        for project_config in projects_config:
            key = project_config.project_type
            if key in self._project_calculation_mapping:
                raise Exception('Duplicate project type found')
            module = importlib.import_module(project_config.processor.module)
            class_ = getattr(module, project_config.processor.class_name)
            self._project_calculation_mapping[key] = class_()

    async def _init_ipfs_client(self):
        """
    Initializes the IPFS client for the current instance.

    If the IPFS singleton instance does not exist, it creates a new instance of the `AsyncIPFSClientSingleton` class using the IPFS settings specified in the application settings. It then initializes the sessions for the IPFS singleton instance.

    The IPFS writer client and IPFS reader client are set to the corresponding clients of the IPFS singleton instance.

    This function should be called before using any IPFS-related functionality in the application.
    """
        if not self._ipfs_singleton:
            self._ipfs_singleton = AsyncIPFSClientSingleton(settings.ipfs)
            await self._ipfs_singleton.init_sessions()
            self._ipfs_writer_client = self._ipfs_singleton._ipfs_write_client
            self._ipfs_reader_client = self._ipfs_singleton._ipfs_read_client

    async def init(self):
        """
    Initializes the necessary components for the application. If the application has not been initialized yet, it initializes the Redis pool, the HTTPX client, the RPC helper, the project calculation mapping, and the IPFS client. Once all the components have been initialized, it sets the `_initialized` flag to True.
    """
        if not self._initialized:
            await self._init_redis_pool()
            await self._init_httpx_client()
            await self._init_rpc_helper()
            await self._init_project_calculation_mapping()
            await self._init_ipfs_client()
        self._initialized = True
