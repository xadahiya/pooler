import contextlib
from functools import wraps

import redis
import redis.exceptions as redis_exc
import tenacity
from redis import asyncio as aioredis
from redis.asyncio.connection import ConnectionPool

from snapshotter.settings.config import settings as settings_conf
from snapshotter.utils.default_logger import logger

# setup logging
logger = logger.bind(module='Powerloom|RedisConn')

REDIS_CONN_CONF = {
    'host': settings_conf.redis.host,
    'port': settings_conf.redis.port,
    'password': settings_conf.redis.password,
    'db': settings_conf.redis.db,
    'retry_on_error': [redis.exceptions.ReadOnlyError],
}


def construct_redis_url():
    if REDIS_CONN_CONF['password']:
        return (
            f'redis://{REDIS_CONN_CONF["password"]}@{REDIS_CONN_CONF["host"]}:{REDIS_CONN_CONF["port"]}'
            f'/{REDIS_CONN_CONF["db"]}'
        )
    else:
        return f'redis://{REDIS_CONN_CONF["host"]}:{REDIS_CONN_CONF["port"]}/{REDIS_CONN_CONF["db"]}'

# ref https://github.com/redis/redis-py/issues/936


async def get_aioredis_pool(pool_size=200):
    pool = ConnectionPool.from_url(
        url=construct_redis_url(),
        retry_on_error=[redis.exceptions.ReadOnlyError],
        max_connections=pool_size,
    )

    return aioredis.Redis(connection_pool=pool)


@contextlib.contextmanager
def create_redis_conn(
    connection_pool: redis.BlockingConnectionPool,
) -> redis.Redis:
    """
    Contextmanager that will create and teardown a session.
    """
    try:
        redis_conn = redis.Redis(connection_pool=connection_pool)
        yield redis_conn
    except redis_exc.RedisError:
        raise
    except KeyboardInterrupt:
        pass


@tenacity.retry(
    stop=tenacity.stop_after_delay(60),
    wait=tenacity.wait_random_exponential(multiplier=1, max=60),
    retry=tenacity.retry_if_exception_type(redis_exc.RedisError),
    reraise=True,
)
def provide_redis_conn(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        arg_conn = 'redis_conn'
        func_params = fn.__code__.co_varnames
        conn_in_args = arg_conn in func_params and func_params.index(
            arg_conn,
        ) < len(args)
        conn_in_kwargs = arg_conn in kwargs
        if conn_in_args or conn_in_kwargs:
            return fn(*args, **kwargs)
        else:
            connection_pool = redis.BlockingConnectionPool(**REDIS_CONN_CONF)

            with create_redis_conn(connection_pool) as redis_obj:
                kwargs[arg_conn] = redis_obj
                logger.debug(
                    'Returning after populating redis connection object',
                )
                return fn(*args, **kwargs)

    return wrapper


def provide_async_redis_conn(fn):
    @wraps(fn)
    async def async_redis_conn_wrapper(*args, **kwargs):
        redis_conn_raw = await kwargs['request'].app.redis_pool.acquire()
        redis_conn = aioredis.Redis(redis_conn_raw)
        kwargs['redis_conn'] = redis_conn
        try:
            return await fn(*args, **kwargs)
        except Exception as e:
            logger.opt(exception=True).error(e)
            return {'error': 'Internal Server Error'}
        finally:
            kwargs['request'].app.redis_pool.release(redis_conn_raw)

    return async_redis_conn_wrapper


# TODO: check wherever this is used and instead
#       attempt to supply the aioredis.Redis object from an instantiated connection pool
def provide_async_redis_conn_insta(fn):
    @wraps(fn)
    async def wrapped(*args, **kwargs):
        arg_conn = 'redis_conn'
        if kwargs.get(arg_conn):
            return await fn(*args, **kwargs)
        else:
            redis_cluster_mode_conn = False
            # try:
            #     if settings_conf.redis.cluster_mode:
            #         redis_cluster_mode_conn = True
            # except:
            #     pass
            if redis_cluster_mode_conn:
                # connection = await aioredis_cluster.create_redis_cluster(
                #     startup_nodes=[(REDIS_CONN_CONF['host'], REDIS_CONN_CONF['port'])],
                #     password=REDIS_CONN_CONF['password'],
                #     pool_maxsize=1,
                #     ssl=REDIS_CONN_CONF['ssl']
                # )
                pass
            else:
                # logging.debug('Creating single connection via high level aioredis interface')
                connection = await aioredis.Redis(
                    host=REDIS_CONN_CONF['host'],
                    port=REDIS_CONN_CONF['port'],
                    db=REDIS_CONN_CONF['db'],
                    password=REDIS_CONN_CONF['password'],
                    retry_on_error=[redis.exceptions.ReadOnlyError],
                )
            kwargs[arg_conn] = connection
            try:
                return await fn(*args, **kwargs)
            except Exception:
                raise
            finally:
                try:  # ignore residual errors
                    await connection.close()
                except:
                    pass

    return wrapped


class RedisPoolCache:
    def __init__(self, pool_size=2000):
        self._aioredis_pool = None
        self._pool_size = pool_size

    async def populate(self):
        if not self._aioredis_pool:
            self._aioredis_pool: aioredis.Redis = await get_aioredis_pool(
                self._pool_size,
            )
