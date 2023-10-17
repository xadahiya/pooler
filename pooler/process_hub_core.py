import json
import os
import threading
import uuid
from multiprocessing import Process
from signal import SIGCHLD
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from threading import Thread
from typing import Dict
from typing import Union

import psutil
import pydantic
import redis
from setproctitle import setproctitle

from pooler.processor_distributor import ProcessorDistributor
from pooler.settings.config import settings
from pooler.system_event_detector import EventDetectorProcess
from pooler.utils.aggregation_worker import AggregationAsyncWorker
from pooler.utils.default_logger import logger
from pooler.utils.exceptions import SelfExitException
from pooler.utils.helper_functions import cleanup_children_procs
from pooler.utils.models.message_models import ProcessHubCommand
from pooler.utils.rabbitmq_helpers import RabbitmqSelectLoopInteractor
from pooler.utils.redis.redis_conn import provide_redis_conn
from pooler.utils.snapshot_worker import SnapshotAsyncWorker

PROC_STR_ID_TO_CLASS_MAP = {
    'SystemEventDetector': {
        'class': EventDetectorProcess,
        'name': 'PowerLoom|SystemEventDetector',
        'target': None,
    },
    'ProcessorDistributor': {
        'class': ProcessorDistributor,
        'name': 'PowerLoom|ProcessorDistributor',
        'target': None,
    },
}


class ProcessHubCore(Process):
    def __init__(self, name, **kwargs):
        """
    Initializes a new instance of the class.

    Args:
        name (str): The name of the instance.
        **kwargs: Additional keyword arguments.

    Attributes:
        _spawned_processes_map (Dict[str, Union[Process, None]]): A dictionary that maps names to spawned processes. The values can be either a Process object or None.
        _spawned_cb_processes_map (Dict[str, Union[Process, None]]): A separate dictionary that maps names to callback worker spawns. The values can be either a Process object or None.
        _thread_shutdown_event (threading.Event): An event object used for thread shutdown.
        _shutdown_initiated (bool): A flag indicating whether the shutdown has been initiated.

    """
        Process.__init__(self, name=name, **kwargs)
        self._spawned_processes_map: Dict[str, Union[Process, None]] = dict()
        self._spawned_cb_processes_map = (
            dict()
        )
        # separate map for callback worker spawns
        self._thread_shutdown_event = threading.Event()
        self._shutdown_initiated = False

    def signal_handler(self, signum, frame):
        """
    Handles signals received by the process.

    Args:
        self (object): The instance of the class.
        signum (int): The signal number.
        frame (object): The current stack frame.

    Returns:
        None

    Raises:
        None

    Notes:
        - This function is called when a signal is received by the process.
        - If the signal is SIGCHLD and the shutdown has not been initiated, it waits for the child process to change state.
        - If the child process has continued or stopped, the function returns.
        - If the child process has been signaled or exited, it logs a debug message and respawns the corresponding callback worker or process.
        - If the signal is SIGINT, SIGTERM, or SIGQUIT, it sets the shutdown flag and stops the RabbitMQ interactor.
    """
        if signum == SIGCHLD and not self._shutdown_initiated:
            pid, status = os.waitpid(
                -1, os.WNOHANG | os.WUNTRACED | os.WCONTINUED,
            )
            if os.WIFCONTINUED(status) or os.WIFSTOPPED(status):
                return
            if os.WIFSIGNALED(status) or os.WIFEXITED(status):
                self._logger.debug(
                    (
                        'Received process crash notification for child process'
                        ' PID: {}'
                    ),
                    pid,
                )
                callback_worker_class = None
                callback_worker_name = None
                callback_worker_unique_id = None
                for (
                    k,
                    worker_unique_id_entries,
                ) in self._spawned_cb_processes_map.items():
                    for (
                        unique_id,
                        worker_process_details,
                    ) in worker_unique_id_entries.items():
                        if worker_process_details['process'].pid == pid:
                            self._logger.debug(
                                (
                                    'Found crashed child process PID in spawned'
                                    ' callback workers | Callback worker class:'
                                    ' {} | Unique worker identifier: {}'
                                ),
                                k,
                                worker_process_details['id'],
                            )
                            callback_worker_name = worker_process_details['id']
                            callback_worker_unique_id = unique_id
                            callback_worker_class = k

                if (
                    callback_worker_name and
                    callback_worker_unique_id
                ):

                    if callback_worker_class == 'snapshot_workers':
                        worker_obj: Process = SnapshotAsyncWorker(
                            name=callback_worker_name,
                        )
                    elif callback_worker_class == 'aggregation_workers':
                        worker_obj: Process = AggregationAsyncWorker(
                            name=callback_worker_name,
                        )

                    worker_obj.start()
                    self._spawned_cb_processes_map[callback_worker_class][
                        callback_worker_unique_id
                    ] = {
                        'id': callback_worker_name,
                        'process': worker_obj,
                    }
                    self._logger.debug(
                        (
                            'Respawned callback worker class {} unique ID {}'
                            ' with PID {} after receiving crash signal against'
                            ' PID {}'
                        ),
                        callback_worker_class,
                        callback_worker_unique_id,
                        worker_obj.pid,
                        pid,
                    )
                    return

                for k, worker_unique_id in self._spawned_processes_map.items():
                    if worker_unique_id != -1 and worker_unique_id.pid == pid:
                        self._logger.debug('RESPAWNING: process for {}', k)
                        proc_details: dict = PROC_STR_ID_TO_CLASS_MAP.get(k)
                        init_kwargs = dict(name=proc_details['name'])
                        if proc_details.get('class'):
                            proc_obj = proc_details['class'](**init_kwargs)
                            proc_obj.start()
                        else:
                            proc_obj = Process(target=proc_details['target'])
                            proc_obj.start()
                        self._logger.debug(
                            'RESPAWNED: process for {} with PID: {}',
                            k,
                            proc_obj.pid,
                        )
                        self._spawned_processes_map[k] = proc_obj
        elif signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._shutdown_initiated = True
            self.rabbitmq_interactor.stop()
            # raise GenericExitOnSignal

    def kill_process(self, pid: int):
        """

    Kills a process with the given process ID (pid).

    Args:
        self: The instance of the class.
        pid (int): The process ID of the process to be killed.

    Returns:
        None

    Raises:
        None

    This function kills the process with the specified process ID by sending a SIGTERM signal. If the process does not terminate within 3 seconds, it sends a SIGKILL signal to forcefully terminate it. Additionally, it waits for any callback processes and spawned processes associated with the given process ID to join.

    Note:
        - This function requires the `psutil` module to be installed.
        - The process ID must be a valid and running process.

    Example:
        >>> kill_process(self, 1234)

    """
        _logger = logger.bind(
            module=f'PowerLoom|ProcessHub|Core:{settings.namespace}-{settings.instance_id}',
        )
        p = psutil.Process(pid)
        _logger.debug(
            'Attempting to send SIGTERM to process ID {} for following command',
            pid,
        )
        p.terminate()
        _logger.debug('Waiting for 3 seconds to confirm termination of process')
        gone, alive = psutil.wait_procs([p], timeout=3)
        for p_ in alive:
            _logger.debug(
                'Process ID {} not terminated by SIGTERM. Sending SIGKILL...',
                p_.pid,
            )
            p_.kill()

        for k, v in self._spawned_cb_processes_map.items():
            for unique_worker_entries in v.values():
                if unique_worker_entries['process'].pid == pid:
                    unique_worker_entries['process'].join()

        for k, v in self._spawned_processes_map.items():
            # internal state reporter might set proc_id_map[k] = -1
            if v != -1 and v.pid == pid:
                v.join()

    @provide_redis_conn
    def internal_state_reporter(self, redis_conn: redis.Redis = None):
        """
    This function is used to report the internal state of a system to a Redis database. It takes a Redis connection object as an optional parameter. If no connection object is provided, it will use the default connection.

    The function runs in a loop until a shutdown event is triggered. Inside the loop, it creates a dictionary called `proc_id_map` to store the process IDs of spawned processes. It iterates over the `_spawned_processes_map` dictionary and adds the process IDs to `proc_id_map`. If a process has not been spawned, it sets the process ID to -1.

    Next, it creates a nested dictionary called `callback_workers` to store the process details of callback workers. It iterates over the `_spawned_cb_processes_map` dictionary and adds the worker details to `callback_workers`. The worker details include the worker's process ID and ID.

    After creating the `callback_workers` dictionary, it converts it to a JSON string using the `json.dumps()` function.

    Finally, it uses the Redis connection object to set the `proc_id_map` dictionary as a hash mapping in the Redis database. The key for the hash mapping is constructed using a specific naming convention.

    If the shutdown event is triggered, it logs an error message and deletes the hash mapping from the Redis database.

    Note: This function is decorated with `@provide_redis_conn`, which suggests that it is part of a larger codebase and requires a Redis connection to be provided.
    """
        while not self._thread_shutdown_event.wait(timeout=2):
            proc_id_map = dict()
            for k, v in self._spawned_processes_map.items():
                if v:
                    proc_id_map[k] = v.pid
                else:
                    proc_id_map[k] = -1
            proc_id_map['callback_workers'] = dict()
            for (
                k,
                unique_worker_entries,
            ) in self._spawned_cb_processes_map.items():
                proc_id_map['callback_workers'][k] = dict()
                for (
                    worker_unique_id,
                    worker_process_details,
                ) in unique_worker_entries.items():
                    proc_id_map['callback_workers'][k][worker_unique_id] = {
                        'pid': worker_process_details['process'].pid,
                        'id': worker_process_details['id'],
                    }
            proc_id_map['callback_workers'] = json.dumps(
                proc_id_map['callback_workers'],
            )
            redis_conn.hset(
                name=f'powerloom:uniswap:{settings.namespace}:{settings.instance_id}:Processes',
                mapping=proc_id_map,
            )
        self._logger.error(
            (
                'Caught thread shutdown notification event. Deleting process'
                ' worker map in redis...'
            ),
        )
        redis_conn.delete(
            f'powerloom:uniswap:{settings.namespace}:{settings.instance_id}:Processes',
        )

    @cleanup_children_procs
    def run(self) -> None:
        """
    This function is a method of a class and it is used to run a process hub core. It starts multiple worker processes for snapshot and aggregation tasks, and also starts a RabbitMQ consumer. The function also sets up signal handlers and starts an internal process state reporter. Finally, it raises a `SelfExitException` to indicate that the process should exit.

    Here is a possible docstring for this function:

    ```
    Runs the process hub core.

    This method starts multiple worker processes for snapshot and aggregation tasks, sets up signal handlers, starts an internal process state reporter, and starts a RabbitMQ consumer. It then raises a `SelfExitException` to indicate that the process should exit.

    Returns:
        None
    ```

    """
        setproctitle('PowerLoom|ProcessHub|Core')
        self._logger = logger.bind(module='PowerLoom|ProcessHub|Core')

        for signame in [SIGINT, SIGTERM, SIGQUIT, SIGCHLD]:
            signal(signame, self.signal_handler)

        self._logger.debug('=' * 80)
        self._logger.debug('Launching Workers')

        # Starting Snapshot workers
        self._spawned_cb_processes_map['snapshot_workers'] = dict()

        for _ in range(settings.callback_worker_config.num_snapshot_workers):
            unique_id = str(uuid.uuid4())[:5]
            unique_name = (
                f'PowerLoom|SnapshotWorker:{settings.namespace}:{settings.instance_id}' +
                '-' +
                unique_id
            )
            snapshot_worker_obj: Process = SnapshotAsyncWorker(name=unique_name)
            snapshot_worker_obj.start()
            self._spawned_cb_processes_map['snapshot_workers'].update(
                {unique_id: {'id': unique_name, 'process': snapshot_worker_obj}},
            )
            self._logger.debug(
                (
                    'Process Hub Core launched process {} for snapshot'
                    ' worker {} with PID: {}'
                ),
                unique_name,
                'snapshot_workers',
                snapshot_worker_obj.pid,
            )

        # Starting Aggregate workers
        self._spawned_cb_processes_map['aggregation_workers'] = dict()

        for _ in range(settings.callback_worker_config.num_aggregation_workers):
            unique_id = str(uuid.uuid4())[:5]
            unique_name = (
                f'PowerLoom|AggregationWorker:{settings.namespace}:{settings.instance_id}' +
                '-' +
                unique_id
            )
            aggregation_worker_obj: Process = AggregationAsyncWorker(name=unique_name)
            aggregation_worker_obj.start()
            self._spawned_cb_processes_map['aggregation_workers'].update(
                {unique_id: {'id': unique_name, 'process': aggregation_worker_obj}},
            )
            self._logger.debug(
                (
                    'Process Hub Core launched process {} for'
                    ' worker {} with PID: {}'
                ),
                unique_name,
                'aggregation_workers',
                aggregation_worker_obj.pid,
            )

        self._logger.debug(
            'Starting Internal Process State reporter for Process Hub Core...',
        )
        self._reporter_thread = Thread(target=self.internal_state_reporter)
        self._reporter_thread.start()
        self._logger.debug('Starting Process Hub Core...')

        queue_name = f'powerloom-processhub-commands-q:{settings.namespace}:{settings.instance_id}'
        self.rabbitmq_interactor: RabbitmqSelectLoopInteractor = RabbitmqSelectLoopInteractor(
            consume_queue_name=queue_name,
            consume_callback=self.callback,
            consumer_worker_name=(
                f'PowerLoom|ProcessHub|Core-{settings.instance_id[:5]}'
            ),
        )
        self._logger.debug('Starting RabbitMQ consumer on queue {}', queue_name)
        self.rabbitmq_interactor.run()
        self._logger.debug('RabbitMQ interactor ioloop ended...')
        self._thread_shutdown_event.set()
        raise SelfExitException

    def callback(self, dont_use_ch, method, properties, body):
        """
    This function is a callback function that handles messages received from a RabbitMQ queue. It takes in several parameters including `dont_use_ch`, `method`, `properties`, and `body`. The function first acknowledges the message using the `basic_ack` method of the RabbitMQ channel. It then parses the message body as JSON and tries to create a `ProcessHubCommand` object from it. If the command is not recognized or fails validation, an error is logged and the function returns.

    If the command is "stop", the function checks if the process ID or process string ID is provided. If the process ID is provided, the function kills the corresponding process. If the process string ID is provided, it checks if it is equal to "self". If it is, the function returns. Otherwise, it looks for the process ID in the core processes string map and the callback processes string map. If the process ID is found in either map, the corresponding process is killed and the map entry is set to None.

    If the command is "start", the function retrieves the process details based on the process string ID from a map. It then creates a new process object or a new `Process` object depending on whether the process details include a class or a target. The process is started and its PID is logged. The process object is then added to the spawned processes map.

    If the command is "restart", the function attempts to kill the process with the provided PID and starts the process with the provided process string ID.

    Note: There are some TODOs in the code that need to be implemented.

    The function does not return any value.

    This function is typically used as a callback for handling messages received from a RabbitMQ queue in a process hub core.
    """
        self.rabbitmq_interactor._channel.basic_ack(
            delivery_tag=method.delivery_tag,
        )
        command = json.loads(body)
        try:
            cmd_json = ProcessHubCommand(**command)
        except pydantic.ValidationError:
            self._logger.error('ProcessHubCore received unrecognized command')
            self._logger.error(command)
            return

        if cmd_json.command == 'stop':
            self._logger.debug(
                'Process Hub Core received stop command: {}', cmd_json,
            )
            process_id = cmd_json.pid
            proc_str_id = cmd_json.proc_str_id
            if process_id:
                return
            if proc_str_id:
                if proc_str_id == 'self':
                    return
                    # self._logger.error('Received stop command on self. Initiating shutdown...')
                    # raise SelfExitException
                mapped_p = self._spawned_processes_map.get(proc_str_id)
                if not mapped_p:
                    self._logger.error(
                        (
                            'Did not find process ID in core processes string'
                            ' map: {}'
                        ),
                        proc_str_id,
                    )
                    mapped_p = self._spawned_cb_processes_map.get(proc_str_id)
                    if not mapped_p:
                        self._logger.error(
                            (
                                'Did not find process ID in callback processes'
                                ' string map: {}'
                            ),
                            proc_str_id,
                        )
                        return
                    else:
                        self.kill_process(mapped_p.pid)
                        self._spawned_cb_processes_map[proc_str_id] = None
                else:
                    self.kill_process(mapped_p.pid)
                    self._spawned_processes_map[proc_str_id] = None

        elif cmd_json.command == 'start':
            try:
                self._logger.debug(
                    'Process Hub Core received start command: {}', cmd_json,
                )
                proc_name = cmd_json.proc_str_id
                self._logger.debug(
                    'Process Hub Core launching process for {}', proc_name,
                )
                proc_details: dict = PROC_STR_ID_TO_CLASS_MAP.get(proc_name)
                init_kwargs = dict(name=proc_details['name'])
                init_kwargs.update(cmd_json.init_kwargs)
                if proc_details.get('class'):
                    proc_obj = proc_details['class'](**init_kwargs)
                    proc_obj.start()
                else:
                    proc_obj = Process(
                        target=proc_details['target'],
                        kwargs=cmd_json.init_kwargs,
                    )
                    proc_obj.start()
                self._logger.debug(
                    'Process Hub Core launched process for {} with PID: {}',
                    proc_name,
                    proc_obj.pid,
                )
                self._spawned_processes_map[proc_name] = proc_obj
            except Exception as err:
                self._logger.opt(exception=True).error(
                    (
                        f'Error while starting a process:{cmd_json} |'
                        f' error_msg: {str(err)}'
                    ),
                )
        elif cmd_json.command == 'restart':
            try:
                # TODO
                self._logger.debug(
                    'Process Hub Core received restart command: {}', cmd_json,
                )
                # first kill
                self._logger.debug(
                    'Attempting to kill process: {}', cmd_json.pid,
                )
                self.kill_process(cmd_json.pid)
                self._logger.debug(
                    'Attempting to start process: {}', cmd_json.proc_str_id,
                )
            except Exception as err:
                self._logger.opt(exception=True).error(
                    (
                        f'Error while restarting a process:{cmd_json} |'
                        f' error_msg: {str(err)}'
                    ),
                )


if __name__ == '__main__':
    p = ProcessHubCore(name='PowerLoom|UniswapPoolerProcessHub|Core')
    p.start()
    while p.is_alive():
        logger.debug(
            'Process hub core is still alive. waiting on it to join...',
        )
        try:
            p.join()
        except:
            pass
