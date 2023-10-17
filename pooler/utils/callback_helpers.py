import asyncio
import hashlib
import json
import time
from abc import ABC
from abc import ABCMeta
from abc import abstractmethod
from abc import abstractproperty
from functools import wraps

import aio_pika
from ipfs_client.main import AsyncIPFSClient
from redis import asyncio as aioredis

from pooler.settings.config import settings
from pooler.utils.default_logger import logger
from pooler.utils.models.data_models import SnapshotterIssue
from pooler.utils.models.message_models import PowerloomCalculateAggregateMessage
from pooler.utils.models.message_models import PowerloomSnapshotProcessMessage
from pooler.utils.models.message_models import PowerloomSnapshotSubmittedMessage
from pooler.utils.rpc import RpcHelper

# setup logger
helper_logger = logger.bind(module='PowerLoom|Callback|Helpers')


async def get_rabbitmq_connection():
    """
    Connects to RabbitMQ server and returns a connection object.

    This function establishes a connection to the RabbitMQ server using the
    specified host, port, virtual host, login, and password. It uses the
    `aio_pika` library to handle the asynchronous connection.

    Returns:
        The connection object to the RabbitMQ server.

    Example:
        connection = await get_rabbitmq_connection()

    """
    return await aio_pika.connect_robust(
        host=settings.rabbitmq.host,
        port=settings.rabbitmq.port,
        virtual_host='/',
        login=settings.rabbitmq.user,
        password=settings.rabbitmq.password,
    )


async def get_rabbitmq_channel(connection_pool) -> aio_pika.Channel:
    """
    Acquires a connection from the connection pool and returns an asynchronous RabbitMQ channel.

    Args:
        connection_pool (aio_pika.Pool): The connection pool to acquire the connection from.

    Returns:
        aio_pika.Channel: The asynchronous RabbitMQ channel.

    Raises:
        None.

    Example:
        connection_pool = aio_pika.Pool(...)
        channel = await get_rabbitmq_channel(connection_pool)

    """
    async with connection_pool.acquire() as connection:
        return await connection.channel()


def misc_notification_callback_result_handler(fut: asyncio.Future):
    """

    Handles the result of a miscellaneous notification callback.

    Args:
        fut (asyncio.Future): The future object representing the result of the callback.

    Returns:
        None

    Raises:
        None

    This function retrieves the result from the future object and logs it. If an exception occurs while retrieving the result, it logs the exception. If the trace_enabled setting is enabled, the exception is logged with the traceback. Otherwise, only the exception message is logged.

    Example usage:
        misc_notification_callback_result_handler(fut)

    """
    try:
        r = fut.result()


except Exception as e:
        if settings.logs.trace_enabled:
            logger.opt(exception=True).error(
                'Exception while sending callback or notification: {}', e,
            )
        else:
            logger.error('Exception while sending callback or notification: {}', e)
    else:
        logger.debug('Callback or notification result:{}', r)


def notify_on_task_failure_snapshot(fn):
    """
    
    Decorator function to notify on task failure snapshot.
    
    This function is a decorator that can be used to wrap around other functions or routes in a web app. It catches any exceptions that occur during the execution of the wrapped function and performs the following actions:
    
    1. Logs the error trace if the trace is enabled in the settings.
    2. Sends the error details to the issue reporting service if the service URL is provided in the settings.
    3. Sends the error details to the Slack channel if the Slack URL is provided in the settings.
    
    Parameters:
    - fn: The function to be wrapped.
    
    Returns:
    - The wrapped function.
    
    Example usage:
    ```
    @notify_on_task_failure_snapshot
    async def my_function(self, *args, **kwargs):
        # Function code here
    ```
    
    """
    @wraps(fn)
    async def wrapper(self, *args, **kwargs):
        """
    This function is a wrapper function that handles exceptions for another function. It logs the error trace and sends the error details to an issue reporting service and a Slack channel if the respective URLs are provided in the settings. The function takes in `self`, `*args`, and `**kwargs` as parameters. The wrapped function is called with the same arguments. The function is asynchronous and uses the `async` keyword.
    """
        try:
            await fn(self, *args, **kwargs)
    except Exception as e:
            # Logging the error trace
            msg_obj: PowerloomSnapshotProcessMessage = kwargs['msg_obj'] if 'msg_obj' in kwargs else args[0]
            task_type: str = kwargs['task_type'] if 'task_type' in kwargs else args[1]
            if settings.logs.trace_enabled:
                logger.opt(exception=True).error(
                    'Error constructing snapshot against message {} for task type {} : {}', msg_obj, task_type, e,
                )
            else:
                logger.error(
                    'Error constructing snapshot against message {} for task type {} : {}', msg_obj, task_type, e,
                )

            # Sending the error details to the issue reporting service
            contract = msg_obj.contract
            epoch_id = msg_obj.epochId
            project_id = f'{task_type}:{contract}:{settings.namespace}'

            if settings.reporting.service_url:
                f = asyncio.ensure_future(
                    self._client.post(
                        url=settings.reporting.service_url,
                        json=SnapshotterIssue(
                            instanceID=settings.instance_id,
                            issueType='MISSED_SNAPSHOT',
                            projectID=project_id,
                            epochId=str(epoch_id),
                            timeOfReporting=str(time.time()),
                            extra=json.dumps({'issueDetails': f'Error : {e}'}),
                        ).dict(),
                    ),
                )
                f.add_done_callback(misc_notification_callback_result_handler)

            if settings.reporting.slack_url:
                f = asyncio.ensure_future(
                    self._client.post(
                        url=settings.reporting.slack_url,
                        json=SnapshotterIssue(
                            instanceID=settings.instance_id,
                            issueType='MISSED_SNAPSHOT',
                            projectID=project_id,
                            epochId=str(epoch_id),
                            timeOfReporting=str(time.time()),
                            extra=json.dumps({'issueDetails': f'Error : {e}'}),
                        ).dict(),
                    ),
                )
                f.add_done_callback(misc_notification_callback_result_handler)

    return wrapper


def notify_on_task_failure_aggregate(fn):
    """
    A decorator function that can be used to notify on task failure and aggregate the error details.
    
    Args:
        fn (function): The function to be decorated.
    
    Returns:
        function: The decorated function.
    
    Raises:
        Exception: If an error occurs while executing the decorated function.
    
    Example:
        @notify_on_task_failure_aggregate
        async def my_function(self, *args, **kwargs):
            # function implementation
    
        In this example, the `my_function` will be decorated with the `notify_on_task_failure_aggregate` decorator,
        which will handle any exceptions that occur during the execution of `my_function` and aggregate the error details.
    
        Note: The decorator assumes that the decorated function is an asynchronous function.
    
    
    """
    @wraps(fn)
    async def wrapper(self, *args, **kwargs):
        """
    This function is a wrapper function that handles exceptions that may occur when executing the wrapped function. It logs the error trace and sends the error details to an issue reporting service and a Slack channel if the corresponding URLs are provided in the settings.
    
    Parameters:
    - self: The instance of the class that the wrapped function belongs to.
    - *args: Positional arguments passed to the wrapped function.
    - **kwargs: Keyword arguments passed to the wrapped function.
    
    Returns:
    - None
    
    Raises:
    - None
    
    Example usage:
    ```python
    @wraps(fn)
    async def wrapper(self, *args, **kwargs):
        try:
            await fn(self, *args, **kwargs)
        except Exception as e:
            # Handle the exception
            ...
    ```
    
    Note: This function can be used as a decorator for other functions or as a middleware for routes in a web application.
    """
        try:
            await fn(self, *args, **kwargs)

    except Exception as e:
            msg_obj = kwargs['msg_obj'] if 'msg_obj' in kwargs else args[0]
            task_type = kwargs['task_type'] if 'task_type' in kwargs else args[1]
            # Logging the error trace
            if settings.logs.trace_enabled:
                logger.opt(exception=True).error(
                    'Error constructing snapshot or aggregate against message {} for task type {} : {}',
                    msg_obj, task_type, e,
                )
            else:
                logger.error(
                    'Error constructing snapshot against message {} for task type {} : {}',
                    msg_obj, task_type, e,
                )

            # Sending the error details to the issue reporting service
            if isinstance(msg_obj, PowerloomCalculateAggregateMessage):
                underlying_project_ids = [project.projectId for project in msg_obj.messages]
                unique_project_id = ''.join(sorted(underlying_project_ids))

                project_hash = hashlib.sha3_256(unique_project_id.encode()).hexdigest()

                project_id = f'{task_type}:{project_hash}:{settings.namespace}'
                epoch_id = msg_obj.epochId

            elif isinstance(msg_obj, PowerloomSnapshotSubmittedMessage):
                contract = msg_obj.projectId.split(':')[-2]
                project_id = f'{task_type}:{contract}:{settings.namespace}'
                epoch_id = msg_obj.epochId

            else:
                project_id = 'UNKNOWN'
                epoch_id = 'UNKNOWN'

            if settings.reporting.service_url:
                f = asyncio.ensure_future(
                    self._client.post(
                        url=settings.reporting.service_url,
                        json=SnapshotterIssue(
                            instanceID=settings.instance_id,
                            issueType='MISSED_SNAPSHOT',
                            projectID=project_id,
                            epochId=str(epoch_id),
                            timeOfReporting=str(time.time()),
                            extra=json.dumps({'issueDetails': f'Error : {e}'}),
                        ).dict(),
                    ),
                )
                f.add_done_callback(misc_notification_callback_result_handler)

            if settings.reporting.slack_url:
                f = asyncio.ensure_future(
                    self._client.post(
                        url=settings.reporting.slack_url,
                        json=SnapshotterIssue(
                            instanceID=settings.instance_id,
                            issueType='MISSED_SNAPSHOT',
                            projectID=project_id,
                            epochId=str(epoch_id),
                            timeOfReporting=str(time.time()),
                            extra=json.dumps({'issueDetails': f'Error : {e}'}),
                        ).dict(),
                    ),
                )
                f.add_done_callback(misc_notification_callback_result_handler)

    return wrapper


class GenericProcessorSnapshot(ABC):
    __metaclass__ = ABCMeta

    def __init__(self):
        """
    Initializes a new instance of the class.
    
    This method is called when a new object of the class is created. It does not take any parameters and does not return any value. It is typically used to set up initial values or perform any necessary setup tasks for the object.
    
    Example:
        obj = ClassName()  # Creates a new object of the class
    
    """
        pass

    @abstractproperty
    def transformation_lambdas(self):
        """
    This function is an abstract property that returns the transformation lambdas. It does not have any implementation and needs to be overridden by the subclasses.
    """
        pass

    @abstractmethod
    async def compute(
        self,
        min_chain_height: int,
        max_chain_height: int,
        data_source_contract_address: str,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
    ):
        """
    Computes something based on the given parameters.
    
    Args:
        min_chain_height (int): The minimum chain height to consider.
        max_chain_height (int): The maximum chain height to consider.
        data_source_contract_address (str): The address of the data source contract.
        redis (aioredis.Redis): The Redis instance to use for caching.
        rpc_helper (RpcHelper): The helper class for making RPC calls.
    
    Returns:
        None
    
    Raises:
        NotImplementedError: This method needs to be implemented by a subclass.
    
    """
        pass


class GenericProcessorSingleProjectAggregate(ABC):
    __metaclass__ = ABCMeta

    def __init__(self):
        """
    Initializes a new instance of the class.
    
    This method is called when a new object of the class is created. It does not take any parameters and does not perform any specific actions. It is used as a placeholder to ensure that the class can be instantiated without any erro
    """
        pass

    @abstractproperty
    def transformation_lambdas(self):
        """
    This function is an abstract property that returns the transformation lambdas. It does not have any implementation and needs to be overridden in the subclasses.
    """
        pass

    @abstractmethod
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
    This function is an abstract method that computes something asynchronously. It takes in several parameters including a message object, a Redis client, RPC helpers, an IPFS client, a protocol state contract, and a project ID. The function does not have an implementation and is meant to be overridden by subclasses.
    """
        pass


class GenericProcessorMultiProjectAggregate(ABC):
    __metaclass__ = ABCMeta

    def __init__(self):
        """
    Initializes a new instance of the class.
    
    This method is called when a new object of the class is created. It does not take any parameters and does not return any value. It is used to perform any necessary setup or initialization tasks for the object.
    
    Example:
        obj = ClassName()  # Creates a new object of the class
    
    """
        pass

    @abstractproperty
    def transformation_lambdas(self):
        """
    This function is an abstract property that returns the transformation lambdas. The transformation lambdas are functions that can be used to transform data. This property should be implemented by subclasses to provide the specific transformation lambdas.
    """
        pass

    @abstractmethod
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
    This function is an abstract method that computes a result based on the given parameters. It takes in a `PowerloomCalculateAggregateMessage` object, a Redis client, two instances of `RpcHelper`, an IPFS client, a protocol state contract, and a project ID as input parameters. 
    
    The function does not have an implementation and is meant to be overridden by subclasses. It is an asynchronous function, indicated by the `async` keyword. 
    
    To use this function, you need to provide the required input parameters and implement the logic inside the function body.
    """
        pass
