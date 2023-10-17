import asyncio
from functools import wraps
from math import floor
from pooler.utils.default_logger import logger


def acquire_bounded_semaphore(fn):
    """

    Acquires a bounded semaphore before executing the decorated function asynchronously. This decorator is designed to be used with asyncio functions.

    Parameters:
        - fn: The function to be decorated.

    Returns:
        - The decorated function.

    Usage:
        - Use this decorator to ensure that only a limited number of coroutines can access a specific resource simultaneously. The decorator acquires a bounded semaphore before executing the decorated function and releases it afterwards.

    Example:
        @acquire_bounded_semaphore
        async def my_async_function(self, *args, **kwargs):
            # Code logic goes here
            pass

    Note:
        - The decorated function should have a 'semaphore' keyword argument, which is an instance of asyncio.BoundedSemaphore. This semaphore determines the maximum number of coroutines that can access the resource simultaneously.
        - If an exception occurs during the execution of the decorated function, it will be logged with the logger and then re-raised.

    """
    @wraps(fn)
    async def wrapped(self, *args, **kwargs):
        """
    A decorator that uses an asyncio semaphore to limit the number of concurrent calls to a function or route.

    Args:
        fn (callable): The function or route to be decorated.

    Returns:
        callable: The decorated function or route.

    Raises:
        Exception: If an error occurs during the acquisition of the semaphore.

    Example:
        @semaphore_decorator
        async def my_function(self, *args, **kwargs):
            # Function logic here

        @semaphore_decorator
        @app.route('/my_route', methods=['GET'])
        async def my_route(self, *args, **kwargs):
            # Route logic here

    The decorated function or route will acquire a semaphore before execution, limiting the number of concurrent calls to the specified value. If an error occurs during the acquisition of the semaphore, an exception will be raised.

    Note:
        The 'semaphore' keyword argument must be passed to the decorated function or route, specifying the asyncio.BoundedSemaphore to be used.
    """
        sem: asyncio.BoundedSemaphore = kwargs['semaphore']
        await sem.acquire()
        result = None
        try:
            result = await fn(self, *args, **kwargs)
    except Exception as e:
            logger.opt(exception=True).error('Error in asyncio semaphore acquisition decorator: {}', e)
        finally:
            sem.release()
            return result
    return wrapped

