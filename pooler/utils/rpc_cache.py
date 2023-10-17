from collections import OrderedDict
from copy import copy
from functools import wraps
from typing import Hashable
from typing import NamedTuple
from typing import Set

from frozendict import frozendict

# Modified https://jellis18.github.io/post/2021-11-25-lru-cache/


class LruCache:
    def __init__(self, capacity: int):
        """
    Initializes a cache object with a specified capacity.

    Args:
        capacity (int): The maximum number of items the cache can hold.

    Attributes:
        capacity (int): The maximum number of items the cache can hold.
        __cache (OrderedDict): An ordered dictionary that stores the cached items.

    Note:
        The cache is implemented using an ordered dictionary to ensure efficient
        retrieval and removal of items based on their access order.
    """
        self.capacity = capacity
        self.__cache: OrderedDict = OrderedDict()

    def get(self, key: Hashable):
        """
    Get the value associated with the given key from the cache.

    Args:
        key (Hashable): The key to retrieve the value for.

    Returns:
        The value associated with the key, or None if the key is not present in the cache.

    Note:
        This method also updates the position of the key in the cache, moving it to the end
        to indicate that it was recently accessed.

    """
        if key not in self.__cache:
            return None
        self.__cache.move_to_end(key)
        return self.__cache[key]

    def insert(self, key: Hashable, value) -> None:
        """
    Inserts a key-value pair into the cache. If the cache is already at its maximum capacity, the least recently used item is removed before inserting the new item. The key-value pair is then added to the cache, and the key is moved to the end of the cache to mark it as the most recently used item.
    """
        if len(self.__cache) == self.capacity:
            self.__cache.popitem(last=False)
        self.__cache[key] = value
        self.__cache.move_to_end(key)

    def __len__(self) -> int:
        """
    This function returns the length of the cache.

    Args:
        self: The cache object.

    Returns:
        The length of the cache as an integer.
    """
        return len(self.__cache)

    def clear(self) -> None:
        """
    Clears the cache by removing all the items stored in it.
    """
        self.__cache.clear()


class CacheInfo(NamedTuple):
    hits: int
    misses: int
    maxsize: int
    currsize: int


class LruCacheRpc:
    def __init__(self, maxsize: int, args: Set):
        """
    Initializes a cache object with a maximum size and a set of arguments.

    Args:
        maxsize (int): The maximum number of items that can be stored in the cache.
        args (Set): A set of arguments that will be used to determine cache hits and misses.
    """
        self.__cache = LruCache(capacity=maxsize)
        self.__hits = 0
        self.__misses = 0
        self.__maxsize = maxsize
        self.__args = args

    def __make_immutable(self, arg):
        """
    This function takes an argument and makes it immutable. If the argument is a dictionary, it returns a frozen dictionary. If the argument is a list, it returns a tuple. If the argument is a set, it returns a frozen set. If the argument is not any of these types, it returns the argument as is.
    """
        if isinstance(arg, dict):
            return frozendict(arg)
        if isinstance(arg, list):
            return tuple(arg)
        if isinstance(arg, set):
            return frozenset(arg)
        return arg

    def __call__(self, fn):
        """

    Decorator for caching the results of a function or a route in a web app.

    Args:
        fn: The function or route to be decorated.

    Returns:
        The decorated function or route.

    Example:
        @cache_decorator
        async def my_function(arg1, arg2):
            # Function body

        @cache_decorator
        @app.route('/my_route', methods=['GET'])
        async def my_route():
            # Route body

    The cache_decorator function wraps the provided function or route with a caching mechanism. It checks if the arguments passed to the function or route have been previously called and stored in the cache. If so, it returns the cached result. If not, it calls the function or route and stores the result in the cache for future use.

    The cache is implemented using a dictionary-like object called __cache. The keys of the cache are generated by hashing the arguments that are marked as cacheable (__args). The cache is stored in memory and can be accessed by other parts of the application.

    The cache_decorator also keeps track of the number of cache hits (__hits) and cache misses (__misses) for performance monitoring purposes.

    Note: This decorator is designed for use with asynchronous functions and routes.

    """
        wraps(fn)

        async def decorated(*args, **kwargs):
            """
    Decorator function that caches the results of an async function.

    Args:
        *args: Positional arguments to be passed to the decorated function.
        **kwargs: Keyword arguments to be passed to the decorated function.

    Returns:
        The result of the decorated function.

    Raises:
        Exception: If an exception occurs while executing the decorated function.

    Notes:
        - This decorator function is intended to be used with async functions.
        - The cache is based on the combination of the decorated function's arguments.
        - If the cache is empty, the decorated function is called and its result is cached.
        - If the cache is not empty, the result is retrieved from the cache.
        - If the result is not found in the cache, the decorated function is called and its result is cached.
        - The cache is implemented using a dictionary-like object called `self.__cache`.
        - The cache key is generated by hashing a tuple of the values of the decorated function's arguments.
        - The cache hit count is stored in `self.__hits`.
        - The cache miss count is stored in `self.__misses`.

    """
            kwargs_data = copy(kwargs)
            kwargs_data.update(zip(fn.__code__.co_varnames, args))
            cache_args = []

            for kwarg in kwargs_data:
                if kwarg in self.__args:
                    cache_args.append(str(kwargs_data[kwarg]))
            cache_args = hash(tuple(cache_args))
            if not cache_args:
                ret = await fn(*args, **kwargs)
            else:
                ret = self.__cache.get(cache_args)
                if ret is None:
                    self.__misses += 1
                    try:
                        ret = await fn(*args, **kwargs)
                        if ret:
                            self.__cache.insert(cache_args, ret)
                    except Exception as e:
                        raise e
                else:
                    self.__hits += 1

            return ret

        return decorated
