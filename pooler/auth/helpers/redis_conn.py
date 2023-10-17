import redis
from redis import asyncio as aioredis

from pooler.auth.conf import auth_settings


def construct_redis_url():
    """
    Constructs a Redis URL based on the authentication settings.

    Returns:
        str: The Redis URL.

    Raises:
        None.

    Example:
        >>> construct_redis_url()
        'redis://<password>@<host>:<port>/<db>'
        or
        'redis://<host>:<port>/<db>' if no password is set.

    """
    if auth_settings.redis.password:
        return (
            f'redis://{auth_settings.redis.password}@{auth_settings.redis.host}:{auth_settings.redis.port}'
            f'/{auth_settings.redis.db}'
        )
    else:
        return f'redis://{auth_settings.redis.host}:{auth_settings.redis.port}/{auth_settings.redis.db}'


async def get_aioredis_pool(pool_size=200):
    """
    Create an asyncio Redis connection pool using aioredis.

    Args:
        pool_size (int, optional): The maximum number of connections in the pool. Defaults to 200.

    Returns:
        aioredis.RedisPool: An asyncio Redis connection pool.

    Example:
        pool = await get_aioredis_pool(pool_size=100)

    """
    return await aioredis.from_url(
        url=construct_redis_url(),
        retry_on_error=[redis.exceptions.ReadOnlyError],
        max_connections=pool_size,
    )


class RedisPoolCache:
    def __init__(self, pool_size=500):
        """
    Initializes an instance of the class with an optional pool size parameter.

    Args:
        pool_size (int, optional): The size of the connection pool. Defaults to 500.
    """
        self._aioredis_pool = None
        self._pool_size = pool_size

    async def populate(self):
        """

    Populates the aioredis pool if it is not already populated.

    Parameters:
    - self: The instance of the class.

    Returns:
    - None

    Raises:
    - None

    """
        if not self._aioredis_pool:
            self._aioredis_pool: aioredis.Redis = await get_aioredis_pool(
                self._pool_size,
            )
