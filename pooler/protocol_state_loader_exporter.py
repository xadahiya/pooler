import asyncio
import bz2
import concurrent.futures
import io
import json
import resource
import sys
from typing import Dict
import loguru
import pydantic
import redis
import uvloop
from collections import defaultdict
from redis import asyncio as aioredis
from web3 import Web3
from pooler.settings.config import settings
from pooler.utils.data_utils import get_project_finalized_cid
from pooler.utils.data_utils import get_project_first_epoch
from pooler.utils.data_utils import w3_get_and_cache_finalized_cid
from pooler.utils.default_logger import logger
from pooler.utils.file_utils import read_json_file
from pooler.utils.models.data_models import ProjectSpecificState
from pooler.utils.models.data_models import ProtocolState
from pooler.utils.redis.redis_conn import REDIS_CONN_CONF
from pooler.utils.redis.redis_conn import RedisPoolCache
from pooler.utils.redis.redis_keys import project_finalized_data_zset
from pooler.utils.redis.redis_keys import project_first_epoch_hmap
from pooler.utils.redis.redis_keys import source_chain_epoch_size_key
from pooler.utils.rpc import RpcHelper
from pooler.utils.utility_functions import acquire_bounded_semaphore


class ProtocolStateLoader:
    _anchor_rpc_helper: RpcHelper
    _redis_conn: aioredis.Redis
    _protocol_state_query_semaphore: asyncio.BoundedSemaphore

    @acquire_bounded_semaphore
    async def _load_finalized_cids_from_contract_in_epoch_range(self, project_id, begin_epoch_id, cur_epoch_id, semaphore):
        """
    This function is used to load finalized CIDs (Content Identifiers) from a contract within a specified epoch range. It takes the following parameters:

    - `project_id`: The ID of the project for which to fetch finalized CIDs.
    - `begin_epoch_id`: The starting epoch ID of the range.
    - `cur_epoch_id`: The current epoch ID.
    - `semaphore`: A bounded semaphore object.

    The function fetches finalized CIDs in batches, where each batch has a size of `epoch_id_fetch_batch_size`. It iterates over the epoch IDs in the specified range and fetches the finalized CID for each epoch using the `w3_get_and_cache_finalized_cid` function. The fetched CIDs are stored in a list `r`.

    If an exception occurs while fetching a CID, an error message is logged. Otherwise, a trace message is logged indicating the successful fetching of the CID.

    Note: This function is decorated with `@acquire_bounded_semaphore`, which suggests that it is used in a concurrent environment where a bounded semaphore is used to limit the number of concurrent executions.

    Example usage:
    ```
    await _load_finalized_cids_from_contract_in_epoch_range(project_id=123, begin_epoch_id=1, cur_epoch_id=10, semaphore=semaphore)
    ```

    Returns:
    This function does not return any value.
    """
        epoch_id_fetch_batch_size = 20
        for e in range(begin_epoch_id, cur_epoch_id + 1, epoch_id_fetch_batch_size):
            self._logger.info(
                'Fetching finalized CIDs for project {} in epoch range {} to {}',
                project_id, e, min(e + epoch_id_fetch_batch_size, cur_epoch_id + 1) - 1,
            )
            r = await asyncio.gather(
                *[
                    w3_get_and_cache_finalized_cid(
                        project_id=project_id,
                        rpc_helper=self._anchor_rpc_helper,
                        epoch_id=epoch_id,
                        redis_conn=self._redis_conn,
                        state_contract_obj=self._protocol_state_contract,
                    )
                    for epoch_id in range(e, min(e + epoch_id_fetch_batch_size, cur_epoch_id + 1))
                ], return_exceptions=True,
            )
            for idx, e in enumerate(r):
                if isinstance(e, Exception):
                    self._logger.error(
                        'Error fetching finalized CIDs for project {} in epoch {}: {}',
                        project_id, begin_epoch_id + idx, e,
                    )
                else:
                    self._logger.trace(
                        'Fetched finalized CIDs for project {} in epoch {}: e',
                        project_id, begin_epoch_id + idx,
                    )

    @acquire_bounded_semaphore
    async def _load_finalized_cids_from_contract(self, project_id, epoch_id_list, semaphore) -> Dict[int, str]:
        """
    This function is used to load finalized CIDs (Content Identifiers) from a contract. It takes in the project ID, a list of epoch IDs, and a semaphore as parameters. The function returns a dictionary mapping epoch IDs to their corresponding finalized CIDs.

    The function starts by defining a batch size of 20 and logging an info message about fetching finalized CIDs for the given project ID and epoch ID list.

    Inside a for loop, the function uses asyncio.gather to concurrently fetch finalized CIDs for each epoch ID in batches. It calls the w3_get_and_cache_finalized_cid function for each epoch ID, passing in the necessary parameters. The results are stored in a list.

    After fetching the finalized CIDs, the function iterates over the results and checks if any exceptions occurred during the fetching process. If an exception occurred, an error message is logged. Otherwise, a trace message is logged indicating that the finalized CID was successfully fetched, and the epoch ID and CID are added to the eid_cid_map dictionary.

    Finally, if there are any epoch IDs that were not fetched successfully, an error message is logged indicating the project ID and the list of epoch IDs that could not be fetched. The function then returns the eid_cid_map dictionary.

    Note: This function is decorated with @acquire_bounded_semaphore, which suggests that it is used as a route handler in a web app.
    """
        batch_size = 20
        self._logger.info(
            'Fetching finalized CIDs for project {} in epoch ID list: {}',
            project_id, epoch_id_list,
        )
        eid_cid_map = dict()
        for i in range(0, len(epoch_id_list), batch_size):
            r = await asyncio.gather(
                *[
                    w3_get_and_cache_finalized_cid(
                        project_id=project_id,
                        rpc_helper=self._anchor_rpc_helper,
                        epoch_id=epoch_id,
                        redis_conn=self._redis_conn,
                        state_contract_obj=self._protocol_state_contract,
                    )
                    for epoch_id in epoch_id_list[i:i + batch_size]
                ], return_exceptions=True,
            )
            for idx, e in enumerate(r):
                if isinstance(e, Exception):
                    self._logger.error(
                        'Error fetching finalized CID for project {} in epoch {}: {}',
                        project_id, epoch_id_list[i + idx], e,
                    )
                else:
                    self._logger.trace(
                        'Fetched finalized CID for project {} in epoch {}',
                        project_id, epoch_id_list[i + idx],
                    )
                    eid_cid_map[epoch_id_list[i + idx]] = e
        self._logger.error('Could not fetch finalized CIDs for project {} against epoch IDs: {}',
                           project_id, list(filter(lambda x: x not in eid_cid_map, epoch_id_list)))
        return eid_cid_map

    async def _init_redis_pool(self):
        """
    Initialize the Redis connection pool.

    This function initializes the Redis connection pool by creating an instance of the RedisPoolCache class with a pool size of 1000. It then populates the pool by calling the `populate` method of the RedisPoolCache instance. Finally, it assigns the Redis connection pool to the `_redis_conn` attribute of the current object.

    Note: This function is asynchronous and should be awaited when called.

    Returns:
        None

    Example:
        await _init_redis_pool()


    """
        self._aioredis_pool = RedisPoolCache(pool_size=1000)
        await self._aioredis_pool.populate()
        self._redis_conn = self._aioredis_pool._aioredis_pool

    async def _init_rpc_helper(self):
        """
    Initializes the RPC helper for the anchor chain.

    This function initializes the RPC helper for the anchor chain by creating an instance of the `RpcHelper` class with the specified RPC settings. It also reads the ABI file for the protocol state from the specified path and assigns it to the `_protocol_state_contract` attribute.

    Parameters:
        self (object): The current instance of the class.

    Returns:
        None

    Example:
        >>> await _init_rpc_helper(self)

    """
        self._anchor_rpc_helper = RpcHelper(rpc_settings=settings.anchor_chain_rpc)
        protocol_abi = read_json_file(settings.protocol_state.abi, self._logger)
        self._protocol_state_contract = self._anchor_rpc_helper.get_current_node()['web3_client'].eth.contract(
            address=Web3.toChecksumAddress(
                settings.protocol_state.address,
            ),
            abi=protocol_abi,
        )

    async def init(self):
        """
    Initializes the object by setting up the logger and initializing the Redis pool and RPC helper. It also creates a bounded semaphore for handling protocol state queries.
    """
        self._logger = logger.bind(
            module=f'PowerLoom|ProtocolStateLoader|{settings.namespace}-{settings.instance_id[:5]}',
        )
        await self._init_redis_pool()
        await self._init_rpc_helper()
        self._protocol_state_query_semaphore = asyncio.BoundedSemaphore(10)

    async def prelim_load(self):
        """

    This function is responsible for loading preliminary data. It is an asynchronous function that initializes the necessary components and performs various queries to retrieve data from the protocol state contract.

    Parameters:
    - self: The instance of the class.

    Returns:
    - cur_epoch_id: The current epoch ID.
    - project_id_first_epoch_id_map: A dictionary mapping project IDs to their corresponding first epoch IDs.
    - all_project_ids: A list of all project IDs.

    The function starts by initializing the necessary components using the `init()` method. It then creates a list of tasks for querying the current epoch ID and all project IDs from the protocol state contract. These tasks are added to the `state_query_call_tasks` list.

    Next, the function calls the `web3_call()` method of the `_anchor_rpc_helper` object to execute the state query tasks asynchronously. The results are stored in the `results` variable.

    The current epoch ID is extracted from the first element of the `results` list. The list of all project IDs is stored in the `all_project_ids` variable.

    The function then creates a list of tasks for querying the first epoch ID for each project. These tasks are created using the `get_project_first_epoch()` function, passing the necessary parameters. The tasks are added to the `project_id_first_epoch_query_tasks` list.

    The `asyncio.gather()` function is used to execute the project first epoch ID query tasks asynchronously. The results are stored in the `project_to_first_epoch_id_results` variable.

    Finally, the function creates a dictionary `project_id_first_epoch_id_map` by mapping each project ID to its corresponding first epoch ID using the `zip()` and `dict()` functions. The function returns the current epoch ID, the project ID to first epoch ID map, and the list of all project IDs.

    """
        await self.init()
        state_query_call_tasks = []
        cur_epoch_id_task = self._protocol_state_contract.functions.currentEpoch()
        state_query_call_tasks.append(cur_epoch_id_task)
        all_project_ids_task = self._protocol_state_contract.functions.getProjects()
        state_query_call_tasks.append(all_project_ids_task)
        results = await self._anchor_rpc_helper.web3_call(state_query_call_tasks, self._redis_conn)
        # print(results)
        # current epoch ID query returned as a list representing the ordered array of elements (begin, end, epochID) of the struct
        # and the other list has only element corresponding to the single level
        # structure of the struct EpochInfo in the contract
        cur_epoch_id = results[0][-1]
        all_project_ids: list = results[1]
        self._logger.debug('Getting first epoch ID against all projects')
        project_id_first_epoch_query_tasks = [
            # get project first epoch ID
            get_project_first_epoch(
                self._redis_conn, self._protocol_state_contract, self._anchor_rpc_helper, project_id,
            ) for project_id in all_project_ids
            # self._protocol_state_contract.functions.projectFirstEpochId(project_id) for project_id in all_project_ids
        ]
        project_to_first_epoch_id_results = await asyncio.gather(*project_id_first_epoch_query_tasks, return_exceptions=True)
        self._logger.debug(
            'Fetched {} results against first epoch IDs successfully', len(
                list(filter(lambda x: x is not None and not isinstance(x, Exception), project_to_first_epoch_id_results)),
            ),
        )
        project_id_first_epoch_id_map = dict(zip(all_project_ids, project_to_first_epoch_id_results))
        return cur_epoch_id, project_id_first_epoch_id_map, all_project_ids

    def _export_project_state(self, project_id, first_epoch_id, end_epoch_id,
                              redis_conn: redis.Redis) -> ProjectSpecificState:
        """
    Exports the state of a project for a given range of epochs.

    Args:
    - project_id (str): The ID of the project.
    - first_epoch_id (int): The ID of the first epoch.
    - end_epoch_id (int): The ID of the last epoch.
    - redis_conn (redis.Redis): The Redis connection object.

    Returns:
    - ProjectSpecificState: The exported project state.

    Note:
    - The exported project state includes the first epoch ID and a dictionary of finalized contract IDs for each epoch within the specified range.
    """

        self._logger.debug('Exporting project state for {}', project_id)
        project_state = ProjectSpecificState.construct()
        project_state.first_epoch_id = first_epoch_id
        self._logger.debug('Project {} first epoch ID: {}', project_id, first_epoch_id)
        project_state.finalized_cids = dict()
        cids_r = redis_conn.zrangebyscore(
            name=project_finalized_data_zset(project_id),
            min=first_epoch_id,
            max=end_epoch_id,
            withscores=True,
        )
        if cids_r:
            [project_state.finalized_cids.update({int(eid): cid}) for cid, eid in cids_r]
        # null_cid_epochs = list(filter(lambda x: 'null' in project_state.finalized_cids[x], project_state.finalized_cids.keys()))
        # # recheck on the contract if they are indeed null
        # self._logger.debug('Verifying CIDs against epoch IDs of project {} by re-fetching state from contract since they were found to be null in local cache: {}', project_id, null_cid_epochs)
        # rechecked_eid_cid_map = asyncio.get_event_loop().run_until_complete(self._load_finalized_cids_from_contract(
        #     project_id, null_cid_epochs, self._protocol_state_query_semaphore,
        # ))
        # project_state.finalized_cids.update(rechecked_eid_cid_map)
        # self._logger.debug('Exported {} finalized CIDs for project {}', len(project_state.finalized_cids), project_id)
        return project_state

    def export(self):
        """
    Exports the state of the protocol to a compressed JSON file. The function retrieves the necessary data from the contract and saves it in the `state.json.bz2` file. The exported state includes information about the current epoch, project-specific states, and finalized contract IDs. The function uses a thread pool executor to concurrently export the state for each project. If any exceptions occur during the export process, they are logged and stored in a dictionary. The function also verifies and updates any null contract IDs found in the local cache by re-fetching the state from the contract. After exporting the state, the function logs a message indicating the successful export.
    """
        asyncio.get_event_loop().run_until_complete(self.prelim_load())
        state = ProtocolState.construct()
        r = redis.Redis(**REDIS_CONN_CONF, max_connections=20, decode_responses=True)
        cur_epoch_id, project_id_first_epoch_id_map, all_project_ids = asyncio.get_event_loop().run_until_complete(self.prelim_load())
        state.synced_till_epoch_id = cur_epoch_id
        state.project_specific_states = dict()
        exceptions = defaultdict()
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_project = {
                executor.submit(
                    self._export_project_state, project_id, project_id_first_epoch_id_map[project_id], cur_epoch_id, r,
                ): project_id for project_id in all_project_ids
            }
        for future in concurrent.futures.as_completed(future_to_project):
            project_id = future_to_project[future]
            try:
                project_specific_state = future.result()
        except Exception as exc:
                exceptions['project'].update({project_id: str(exc)})
            else:
                null_cid_epochs = list(filter(lambda x: 'null' in project_specific_state.finalized_cids[x], project_specific_state.finalized_cids.keys()))
                # recheck on the contract if they are indeed null
                self._logger.debug('Verifying CIDs against epoch IDs of project {} by re-fetching state from contract since they were found to be null in local cache: {}', project_id, null_cid_epochs)
                rechecked_eid_cid_map = asyncio.get_event_loop().run_until_complete(self._load_finalized_cids_from_contract(
                    project_id=project_id, epoch_id_list=null_cid_epochs, semaphore=self._protocol_state_query_semaphore,
                ))
                project_specific_state.finalized_cids.update(rechecked_eid_cid_map)
                self._logger.debug('Exported {} finalized CIDs for project {}', len(project_specific_state.finalized_cids), project_id)
                state.project_specific_states[project_id] = project_specific_state
        state_json = state.json()
        with bz2.open('state.json.bz2', 'wb') as f:
            with io.TextIOWrapper(f, encoding='utf-8') as enc:
                enc.write(state_json)
        self._logger.info('Exported state.json.bz2')

    def _load_project_state(self, project_id, project_state: ProjectSpecificState, redis_conn: redis.Redis):
        """
    Loads the project state for a specific project.
    
    Args:
        project_id (str): The ID of the project.
        project_state (ProjectSpecificState): The specific state of the project.
        redis_conn (redis.Redis): The Redis connection object.
    
    Returns:
        None
    
    Raises:
        Exception: If there is an error while loading the finalized CIDs for the project.
    
    The function loads the project state by performing the following steps:
    1. Sets the first epoch ID for the project in the Redis hash map.
    2. Loads the finalized CIDs for the project into a Redis sorted set.
    3. Logs the loaded first epoch ID and the number of loaded finalized CIDs.
    
    If there is an error while loading the finalized CIDs, an exception is raised and logged.
    
    Note:
    - The function assumes that the Redis connection object is already established.
    - The function uses the logger object to log debug and error messages.
    - The function is intended to be used internally within the project class.
    
    """
        self._logger.debug('Loading project state for {}', project_id)
        redis_conn.hset(project_first_epoch_hmap(), project_id, project_state.first_epoch_id)
        self._logger.debug('Loaded first epoch ID {} for project {}', project_state.first_epoch_id, project_id)
        try:
            s = redis_conn.zadd(
                name=project_finalized_data_zset(project_id),
                mapping={v: k for k, v in project_state.finalized_cids.items()},
            )
    except:
            self._logger.error('Error while loading finalized CIDs for project {}', project_id)
        else:
            self._logger.debug('Loaded {} finalized CIDs for project {}', s, project_id)

    def load(self, file_name='state.json.bz2'):
        """
    Loads the state from a file in JSON format and initializes the protocol state. The file name is optional and defaults to 'state.json.bz2'. The function uses the asyncio library to run the initialization process. It also connects to a Redis server using the provided connection configuration and sets the maximum number of connections to 20. The function then reads the state JSON from the file using the bz2 library. If there is an error while parsing the state JSON, an error message is logged and the function returns without further processing. If the state JSON is successfully parsed, the function uses a ThreadPoolExecutor with a maximum of 10 workers to load the project-specific states in parallel. Each project-specific state is loaded using the _load_project_state method and the corresponding Redis connection. Any errors that occur during the loading process are logged. Finally, a debug message is logged to indicate that the state has been loaded from the file.
    """
        asyncio.get_event_loop().run_until_complete(self.init())
        r = redis.Redis(**REDIS_CONN_CONF, max_connections=20, decode_responses=True)
        self._logger.debug('Loading state from file {}', file_name)
        with bz2.open(file_name, 'rb') as f:
            state_json = f.read()
        try:
            state = ProtocolState.parse_raw(state_json)
        except pydantic.ValidationError as e:
            self._logger.opt(exception=True).error('Error while parsing state file: {}', e)
            with open('state_parse_error.json', 'w') as f:
                json.dump(e.errors(), f)
            return
        self._logger.debug('Loading state from file {}', file_name)
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_project = {
                executor.submit(
                    self._load_project_state, project_id, ProjectSpecificState.parse_obj(
                        project_state,
                    ), r,
                ): project_id for project_id, project_state in state.project_specific_states.items()
            }
        for future in concurrent.futures.as_completed(future_to_project):
            project_id = future_to_project[future]
            try:
                project_specific_state = future.result()
        except Exception as exc:
                self._logger.opt(exception=True).error('Error while loading project state for {}: {}', project_id, exc)
            else:
                self._logger.debug('Loaded project state for {}', project_id)
        self._logger.debug('Loaded state from file {}', file_name)


if __name__ == '__main__':
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(
        resource.RLIMIT_NOFILE,
        (settings.rlimit.file_descriptors, hard),
    )
    state_loader_exporter = ProtocolStateLoader()
    asyncio.get_event_loop().run_until_complete(state_loader_exporter.init())
    if sys.argv[1] == 'export':
        ProtocolStateLoader().export()
    elif sys.argv[1] == 'load':
        ProtocolStateLoader().load()
