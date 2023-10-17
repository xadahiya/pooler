import time

from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
from redis import asyncio as aioredis

from pooler.auth.helpers.data_models import AddApiKeyRequest
from pooler.auth.helpers.data_models import AppOwnerModel
from pooler.auth.helpers.data_models import UserAllDetailsResponse
from pooler.auth.helpers.redis_conn import RedisPoolCache
from pooler.auth.helpers.redis_keys import all_users_set
from pooler.auth.helpers.redis_keys import api_key_to_owner_key
from pooler.auth.helpers.redis_keys import user_active_api_keys_set
from pooler.auth.helpers.redis_keys import user_details_htable
from pooler.auth.helpers.redis_keys import user_revoked_api_keys_set
from pooler.settings.config import settings
from pooler.utils.default_logger import logger

# setup logging
api_logger = logger.bind(module=__name__)

# setup CORS origins stuff
origins = ['*']
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.on_event('startup')
async def startup_boilerplate():
    """
    This function is a startup event handler for a web app. It initializes and populates a Redis connection pool and sets it as a state variable in the app. It also sets the app's core settings.

    Args:
        None

    Returns:
        None

    Raises:
        None

    Example:
        ```
        @app.on_event('startup')
        async def startup_boilerplate():
            app.state.aioredis_pool = RedisPoolCache(pool_size=100)
            await app.state.aioredis_pool.populate()
            app.state.redis_pool = app.state.aioredis_pool._aioredis_pool
            app.state.core_settings = settings
        ```
    """
    app.state.aioredis_pool = RedisPoolCache(pool_size=100)
    await app.state.aioredis_pool.populate()
    app.state.redis_pool = app.state.aioredis_pool._aioredis_pool
    app.state.core_settings = settings


@app.post('/user')
async def create_update_user(
    request: Request,
    user_cu_request: AppOwnerModel,
    response: Response,
):
    """
    Creates or updates a user in the application.

    Args:
        request (Request): The incoming request object.
        user_cu_request (AppOwnerModel): The user details to create or update.
        response (Response): The outgoing response object.

    Returns:
        dict: A dictionary indicating the success of the operation. If the operation is successful, the dictionary will have a key 'success' with a value of True. Otherwise, the dictionary will have a key 'success' with a value of False.

    Raises:
        None.

    Note:
        - This function assumes that a Redis connection pool is available in the application's state.
        - The function uses Redis to store user details and maintain a set of all users.
        - If the user is being created for the first time, the 'next_reset_at' field of the user details will be set to the current timestamp plus 86400 seconds (24 hours).
        - If an error occurs during the operation, an exception will be logged and the function will return a dictionary with 'success' set to False.
    """
    redis_conn: aioredis.Redis = request.app.state.redis_pool
    try:
        await redis_conn.sadd(
            all_users_set(),
            user_cu_request.email,
        )
        if not await redis_conn.sismember(
            all_users_set(),
            user_cu_request.email,
        ):
            user_cu_request.next_reset_at = int(time.time()) + 86400
        user_details = user_cu_request.dict()
        await redis_conn.hset(
            name=user_details_htable(user_cu_request.email),
            mapping=user_details,
        )
except Exception as e:
        api_logger.opt(exception=True).error('{}', e)
        return {'success': False}
    else:
        return {'success': True}


@app.post('/user/{email}/api_key')
async def add_api_key(
    api_key_request: AddApiKeyRequest,
    email: str,
    request: Request,
    response: Response,
):
    """
    Adds an API key for a user.
    
    Args:
        api_key_request (AddApiKeyRequest): The request object containing the API key to be added.
        email (str): The email of the user.
        request (Request): The request object.
        response (Response): The response object.
    
    Returns:
        dict: A dictionary with the following keys:
            - success (bool): Indicates if the API key was added successfully.
            - error (str): The error message if the user does not exist.
    
    Raises:
        None
    
    Example:
        >>> api_key_request = AddApiKeyRequest(api_key='1234567890')
        >>> email = 'example@example.com'
        >>> request = Request()
        >>> response = Response()
        >>> add_api_key(api_key_request, email, request, response)
        {'success': True}
    
    Note:
        This function requires a Redis connection to be available in the application state.
    
    """
    redis_conn: aioredis.Redis = request.app.state.redis_pool
    if not await redis_conn.sismember(all_users_set(), email):
        return {'success': False, 'error': 'User does not exists'}

    async with redis_conn.pipeline(transaction=True) as p:
        await p.sadd(
            user_active_api_keys_set(email),
            api_key_request.api_key,
        ).set(api_key_to_owner_key(api_key_request.api_key), email).execute()
    return {'success': True}


@app.delete('/user/{email}/api_key')
async def revoke_api_key(
    api_key_request: AddApiKeyRequest,
    email: str,
    request: Request,
    response: Response,
):
    """
    
    Revoke API key for a user.
    
    This route is used to revoke an API key for a specific user. The API key is provided in the `api_key_request` parameter, and the user's email is provided in the `email` path parameter.
    
    Parameters:
    - `api_key_request` (AddApiKeyRequest): The request object containing the API key to be revoked.
    - `email` (str): The email of the user for whom the API key should be revoked.
    - `request` (Request): The HTTP request object.
    - `response` (Response): The HTTP response object.
    
    Returns:
    - A dictionary with the following keys:
      - `success` (bool): Indicates whether the API key was successfully revoked.
      - `error` (str): If `success` is False, this contains an error message explaining the reason for failure.
    
    Example:
    ```
    {
        "success": True
    }
    ```
    
    Note:
    - If the user does not exist, the function will return `{'success': False, 'error': 'User does not exist'}`.
    - If the API key is not active, the function will return `{'success': False, 'error': 'API key not active'}`.
    - If the API key has already been revoked, the function will return `{'success': False, 'error': 'API key already revoked'}`.
    
    """
    redis_conn: aioredis.Redis = request.app.state.redis_pool
    if not await redis_conn.sismember(all_users_set(), email):
        return {'success': False, 'error': 'User does not exists'}

    if not await redis_conn.sismember(
        user_active_api_keys_set(email),
        api_key_request.api_key,
    ):
        return {'success': False, 'error': 'API key not active'}
    elif await redis_conn.sismember(
        user_revoked_api_keys_set(email),
        api_key_request.api_key,
    ):
        return {'success': False, 'error': 'API key already revoked'}
    await redis_conn.smove(
        user_active_api_keys_set(email),
        user_revoked_api_keys_set(email),
        api_key_request.api_key,
    )
    return {'success': True}


@app.get('/user/{email}')
async def get_user_details(
    request: Request,
    response: Response,
    email: str,
):
    """
    
    Retrieves the details of a user based on their email address.
    
    Parameters:
        - request (Request): The incoming request object.
        - response (Response): The outgoing response object.
        - email (str): The email address of the user.
    
    Returns:
        - dict: A dictionary containing the user details if the user exists, 
                otherwise a dictionary with 'success' set to False and an error message.
    
    The function retrieves the user details from a Redis database using the provided email address. 
    If the user does not exist, it returns a dictionary with 'success' set to False and an error message. 
    If the user exists, it retrieves the active and revoked API keys associated with the user and returns 
    a dictionary with 'success' set to True and the user details along with the active and revoked API keys.
    
    Note: This function is an asynchronous function and should be awaited when called.
    
    """
    redis_conn: aioredis.Redis = request.app.state.redis_pool

    all_details = await redis_conn.hgetall(name=user_details_htable(email))
    if not all_details:
        return {'success': False, 'error': 'User does not exists'}

    active_api_keys = await redis_conn.smembers(
        name=user_active_api_keys_set(email),
    )
    revoked_api_keys = await redis_conn.smembers(
        name=user_revoked_api_keys_set(email),
    )

    return {
        'success': True,
        'data': UserAllDetailsResponse(
            **{k.decode('utf-8'): v.decode('utf-8') for k, v in all_details.items()},
            active_api_keys=[x.decode('utf-8') for x in active_api_keys],
            revoked_api_keys=[x.decode('utf-8') for x in revoked_api_keys],
        ).dict(),
    }


@app.get('/users')
async def get_all_users(
    request: Request,
    response: Response,
):
    """
    
    Get all users from the Redis database.
    
    Args:
        request (Request): The incoming request object.
        response (Response): The outgoing response object.
    
    Returns:
        dict: A dictionary containing the success status and the list of all users.
    
    Example:
        >>> get_all_users(request, response)
        {
            'success': True,
            'data': ['user1', 'user2', 'user3']
        }
    
    """
    redis_conn: aioredis.Redis = request.app.state.redis_pool
    all_users = await redis_conn.smembers(all_users_set())
    return {
        'success': True,
        'data': [x.decode('utf-8') for x in all_users],
    }
