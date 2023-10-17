import aio_pika
import pika

from pooler.settings.config import settings
from pooler.utils.default_logger import logger

# setup logging
init_rmq_logger = logger.bind(module='PowerLoom|RabbitMQ|Init')


def create_rabbitmq_conn() -> pika.BlockingConnection:
    """
    Creates a connection to RabbitMQ server using the provided settings.

    Returns:
        pika.BlockingConnection: A blocking connection object to RabbitMQ server.

    Raises:
        pika.exceptions.AMQPConnectionError: If there is an error connecting to the RabbitMQ server.

    Example:
        conn = create_rabbitmq_conn()
        channel = conn.channel()
    """
    c = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=settings.rabbitmq.host,
            port=settings.rabbitmq.port,
            virtual_host='/',
            credentials=pika.PlainCredentials(
                username=settings.rabbitmq.user,
                password=settings.rabbitmq.password,
            ),
            heartbeat=30,
        ),
    )
    return c


async def get_rabbitmq_connection_async() -> aio_pika.Connection:
    """
    Connects to RabbitMQ server asynchronously and returns a connection object.

    This function establishes a connection to the RabbitMQ server using the specified
    host, port, virtual host, login, and password. It returns a connection object
    that can be used to interact with the RabbitMQ server asynchronously.

    Returns:
        aio_pika.Connection: A connection object representing the connection to the
        RabbitMQ server.


    """
    return await aio_pika.connect_robust(
        host=settings.rabbitmq.host,
        port=settings.rabbitmq.port,
        virtual_host='/',
        login=settings.rabbitmq.user,
        password=settings.rabbitmq.password,
    )


def processhub_command_publish(
    ch: pika.adapters.blocking_connection.BlockingChannel, cmd: str,
) -> None:
    """
    Publishes a command to the processhub-commands exchange in RabbitMQ.

    Args:
        ch (pika.adapters.blocking_connection.BlockingChannel): The RabbitMQ channel to use for publishing.
        cmd (str): The command to publish.

    Returns:
        None

    Raises:
        None
    """
    ch.basic_publish(
        exchange=(
            f'{settings.rabbitmq.setup.core.exchange}:{settings.namespace}'
        ),
        routing_key=(
            f'processhub-commands:{settings.namespace}:{settings.instance_id}'
        ),
        body=cmd.encode('utf-8'),
        properties=pika.BasicProperties(
            delivery_mode=2,
            content_type='text/plain',
            content_encoding='utf-8',
        ),
        mandatory=True,
    )


def init_topic_queue(
    ch: pika.adapters.blocking_connection.BlockingChannel,
    exchange_name: str,
    queue_name: str,
    routing_key_pattern: str,
) -> None:
    """
    Initializes a topic queue in RabbitMQ.

    Args:
        ch (pika.adapters.blocking_connection.BlockingChannel): The RabbitMQ channel to use.
        exchange_name (str): The name of the exchange to declare.
        queue_name (str): The name of the queue to initialize.
        routing_key_pattern (str): The routing key pattern to bind the queue to.

    Returns:
        None

    Raises:
        None

    Example:
        init_topic_queue(ch, 'my_exchange', 'my_queue', 'my.routing.key.*')
    """
    ch.exchange_declare(
        exchange=exchange_name, exchange_type='topic', durable=True,
    )
    init_rmq_logger.debug(
        'Initialized RabbitMQ Topic exchange: {}', exchange_name,
    )
    init_queue(
        ch,
        queue_name=queue_name,
        routing_key=routing_key_pattern,
        exchange_name=exchange_name,
    )


def get_snapshot_queue_routing_key_pattern() -> str:
    """
    This function returns the queue name and routing key pattern for a snapshot queue in the Powerloom backend. The queue name is generated based on the namespace and instance ID settings. The routing key pattern is also generated based on the namespace and instance ID settings, with the addition of the string "EpochReleased" followed by any characters. The function returns both the queue name and routing key pattern as a tuple.
    """
    queue_name = (
        f'powerloom-backend-cb-snapshot:{settings.namespace}:{settings.instance_id}'
    )
    routing_key_pattern = f'powerloom-backend-callback:{settings.namespace}:{settings.instance_id}:EpochReleased.*'
    return queue_name, routing_key_pattern


def get_aggregate_queue_routing_key_pattern() -> str:
    """
    This function returns a tuple containing the queue name and the routing key pattern for the aggregate queue in a Powerloom backend. The queue name is generated using the namespace and instance ID from the settings. The routing key pattern is also generated using the namespace and instance ID, with the addition of the string "CalculateAggregate" followed by any characters.

    Returns:
        - queue_name (str): The name of the aggregate queue.
        - routing_key_pattern (str): The pattern for routing keys to be matched by the aggregate queue.
    """
    queue_name = (
        f'powerloom-backend-cb-aggregate:{settings.namespace}:{settings.instance_id}'
    )
    routing_key_pattern = f'powerloom-backend-callback:{settings.namespace}:{settings.instance_id}:CalculateAggregate.*'
    return queue_name, routing_key_pattern


def init_callback_queue(
    ch: pika.adapters.blocking_connection.BlockingChannel,
) -> None:
    """
    This function initializes the callback queue for a given channel in a RabbitMQ connection. It sets up two types of queues: snapshot queue and aggregate queue. The snapshot queue is used for storing snapshots of data, while the aggregate queue is used for aggregating data.

    Parameters:
    - ch: The RabbitMQ channel to initialize the callback queue on.

    Returns:
    - None

    Example usage:
    ```
    ch = pika.BlockingConnection().channel()
    init_callback_queue(ch)
    ```

    Note: This function assumes that the necessary routing key patterns and exchange names are already defined in the `settings` module.
    """
    callback_exchange_name = (
        f'{settings.rabbitmq.setup.callbacks.exchange}:{settings.namespace}'
    )
    # Snapshot queue
    queue_name, routing_key_pattern = get_snapshot_queue_routing_key_pattern()
    init_topic_queue(
        ch,
        exchange_name=callback_exchange_name,
        queue_name=queue_name,
        routing_key_pattern=routing_key_pattern,
    )

    # Aggregate queue
    queue_name, routing_key_pattern = get_aggregate_queue_routing_key_pattern()
    init_topic_queue(
        ch,
        exchange_name=callback_exchange_name,
        queue_name=queue_name,
        routing_key_pattern=routing_key_pattern,
    )


def init_commit_payload_queue(
    ch: pika.adapters.blocking_connection.BlockingChannel,
) -> None:
    """
    This function initializes a queue for handling commit payloads in a RabbitMQ messaging system. It takes a `ch` parameter, which is a blocking channel object from the `pika.adapters.blocking_connection` module. The function does not return anything (`None`).

    The function first constructs the name of the exchange to be used for the callback. It combines the `settings.rabbitmq.setup.commit_payload.exchange` value with the `settings.namespace` value.

    Next, it constructs a routing key pattern based on the `settings.namespace` and `settings.instance_id` values. The pattern is in the format: `powerloom-backend-commit-payload:{namespace}:{instance_id}.*`.

    Then, it constructs the name of the queue to be created. The name is in the format: `powerloom-backend-commit-payload-queue:{namespace}:{instance_id}`.

    Finally, the function calls another function called `init_topic_queue` to initialize the topic queue. It passes the blocking channel object (`ch`), the exchange name (`callback_exchange_name`), the queue name (`queue_name`), and the routing key pattern (`routing_key_pattern`) as parameters to this function.

    Note: This function is likely used in a web application as a route handler.
    """
    callback_exchange_name = (
        f'{settings.rabbitmq.setup.commit_payload.exchange}:{settings.namespace}'
    )
    routing_key_pattern = f'powerloom-backend-commit-payload:{settings.namespace}:{settings.instance_id}.*'
    queue_name = (
        f'powerloom-backend-commit-payload-queue:{settings.namespace}:{settings.instance_id}'
    )
    init_topic_queue(
        ch,
        exchange_name=callback_exchange_name,
        queue_name=queue_name,
        routing_key_pattern=routing_key_pattern,
    )


def init_event_detector_queue(
    ch: pika.adapters.blocking_connection.BlockingChannel,
) -> None:
    """
    Initializes the event detector queue.

    Args:
        ch (pika.adapters.blocking_connection.BlockingChannel): The channel to communicate with RabbitMQ.

    Returns:
        None

    The function initializes the event detector queue by setting up the exchange name, routing key pattern, and queue name based on the provided settings. It then calls the `init_topic_queue` function to create the queue and bind it to the exchange using the specified routing key pattern.

    """
    event_detector_exchange_name = (
        f'{settings.rabbitmq.setup.event_detector.exchange}:{settings.namespace}'
    )
    routing_key_pattern = f'powerloom-event-detector:{settings.namespace}:{settings.instance_id}.*'
    queue_name = (
        f'powerloom-event-detector:{settings.namespace}:{settings.instance_id}'
    )
    init_topic_queue(
        ch,
        exchange_name=event_detector_exchange_name,
        queue_name=queue_name,
        routing_key_pattern=routing_key_pattern,
    )


def init_queue(
    ch: pika.adapters.blocking_connection.BlockingChannel,
    queue_name: str,
    routing_key: str,
    exchange_name: str,
) -> None:
    """
    Initializes a queue in RabbitMQ with the specified parameters.

    Args:
        ch (pika.adapters.blocking_connection.BlockingChannel): The RabbitMQ channel to use for the operation.
        queue_name (str): The name of the queue to declare.
        routing_key (str): The routing key to bind the queue to.
        exchange_name (str): The name of the exchange to bind the queue to.

    Returns:
        None

    Raises:
        None

    Example:
        init_queue(ch, "my_queue", "my_routing_key", "my_exchange")

    Note:
        This function assumes that the RabbitMQ connection has already been established.
    """
    ch.queue_declare(queue_name)
    ch.queue_bind(
        exchange=exchange_name, queue=queue_name, routing_key=routing_key,
    )
    init_rmq_logger.debug(
        (
            'Initialized RabbitMQ setup | Queue: {} | Exchange: {} | Routing'
            ' Key: {}'
        ),
        queue_name,
        exchange_name,
        routing_key,
    )


def init_exchanges_queues():
    """
    Initializes exchanges and queues for RabbitMQ.

    Creates a RabbitMQ connection and a channel. Declares a direct exchange with the name "{core_exchange}:{namespace}".

    Initializes the following queues:
    - "powerloom-processhub-commands-q" with routing key "processhub-commands"
    - Callback queue
    - Event detector queue
    - Commit payload queue

    Each queue name and routing key is appended with the namespace and instance ID to support multiple pooler instances sharing the same RabbitMQ setup and broker.
    """
    c = create_rabbitmq_conn()
    ch: pika.adapters.blocking_connection.BlockingChannel = c.channel()
    # core exchange remains same for multiple pooler instances in the namespace to share across different instance IDs
    exchange_name = (
        f'{settings.rabbitmq.setup.core.exchange}:{settings.namespace}'
    )
    ch.exchange_declare(
        exchange=exchange_name, exchange_type='direct', durable=True,
    )
    init_rmq_logger.debug(
        'Initialized RabbitMQ Direct exchange: {}', exchange_name,
    )

    to_be_inited = [
        ('powerloom-processhub-commands-q', 'processhub-commands'),
    ]
    for queue_name, routing_key in to_be_inited:
        # add namespace and instance ID to facilitate multiple pooler instances sharing same rabbitmq setup and broker
        q = f'{queue_name}:{settings.namespace}:{settings.instance_id}'
        r = f'{routing_key}:{settings.namespace}:{settings.instance_id}'
        init_queue(ch, q, r, exchange_name)

    init_callback_queue(ch)
    init_event_detector_queue(ch)
    init_commit_payload_queue(ch)


if __name__ == '__main__':
    init_exchanges_queues()
