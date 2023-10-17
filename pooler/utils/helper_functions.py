import sys
from functools import wraps

from pooler.utils.default_logger import logger

# setup logging
logger = logger.bind(module='PowerLoom|HelperFunctions')


def cleanup_children_procs(fn):
    """

    This function is a decorator that wraps around another function. It is used to handle exceptions that may occur during the execution of the wrapped function.

    Parameters:
    - fn: The function to be wrapped.

    Returns:
    - The wrapped function.

    The wrapped function executes the wrapped function `fn` and logs a message indicating that the process hub core has finished running. If an exception occurs during the execution of `fn`, it is logged with the corresponding error message.

    After handling the exception, the function waits for any spawned callback workers and spawned core workers to join. It logs messages indicating the progress of waiting for each worker to join.

    Finally, it logs a message indicating that it has finished waiting for all children and exits the program.

    Note: This function assumes the existence of a logger object and other variables (`logger`, `self._spawned_cb_processes_map`, `self._spawned_processes_map`, `self._reporter_thread`) that are not defined within the function.

    """
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        """
    This function is a wrapper function that wraps around another function. It executes the wrapped function and logs the completion of the process. If an exception occurs during the execution, it logs the exception and waits for any spawned callback workers and core workers to join before exiting.

    Parameters:
    - `self`: The instance of the class that the function belongs to.
    - `*args`: Variable length argument list.
    - `**kwargs`: Arbitrary keyword arguments.

    Returns:
    - None

    Raises:
    - None

    Example Usage:
    ```python
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            fn(self, *args, **kwargs)
            logger.info('Finished running process hub core...')
        except Exception as e:
            logger.opt(exception=True).error(
                'Received an exception on process hub core run(): {}',
                e,
            )
            # Rest of the code...
    ```

    Note: This function is designed to be used as a decorator for other functions or methods.
    """
        try:
            fn(self, *args, **kwargs)
            logger.info('Finished running process hub core...')
    except Exception as e:
            logger.opt(exception=True).error(
                'Received an exception on process hub core run(): {}',
                e,
            )
            # logger.error('Initiating kill children....')
            # # silently kill all children
            # procs = psutil.Process().children()
            # for p in procs:
            #     p.terminate()
            # gone, alive = psutil.wait_procs(procs, timeout=3)
            # for p in alive:
            #     logger.error(f'killing process: {p.name()}')
            #     p.kill()
            logger.error('Waiting on spawned callback workers to join...')
            for (
                worker_class_name,
                unique_worker_entries,
            ) in self._spawned_cb_processes_map.items():
                for (
                    worker_unique_id,
                    worker_unique_process_details,
                ) in unique_worker_entries.items():
                    if worker_unique_process_details['process'].pid:
                        logger.error(
                            (
                                'Waiting on spawned callback worker {} | Unique'
                                ' ID {} | PID {}  to join...'
                            ),
                            worker_class_name,
                            worker_unique_id,
                            worker_unique_process_details['process'].pid,
                        )
                        worker_unique_process_details['process'].join()

            logger.error(
                'Waiting on spawned core workers to join... {}',
                self._spawned_processes_map,
            )
            for (
                worker_class_name,
                unique_worker_entries,
            ) in self._spawned_processes_map.items():
                logger.error(
                    'spawned Process Pid to wait on {}',
                    unique_worker_entries.pid,
                )
                # internal state reporter might set proc_id_map[k] = -1
                if unique_worker_entries != -1:
                    logger.error(
                        (
                            'Waiting on spawned core worker {} | PID {}  to'
                            ' join...'
                        ),
                        worker_class_name,
                        unique_worker_entries.pid,
                    )
                    unique_worker_entries.join()
            logger.error('Finished waiting for all children...now can exit.')
        finally:
            logger.error('Finished waiting for all children...now can exit.')
            self._reporter_thread.join()
            sys.exit(0)
            # sys.exit(0)

    return wrapper


def acquire_threading_semaphore(fn):
    """
    
    Acquires a threading semaphore before executing the decorated function and releases it afterwards. This decorator is used to control access to a shared resource using a semaphore.
    
    Parameters:
        - fn: The function to be decorated.
        
    Returns:
        - The decorated function.
        
    Example Usage:
        @acquire_threading_semaphore
        def my_function(semaphore, arg1, arg2):
            # Function body
            
        my_function(semaphore, arg1, arg2)
    
    """
    @wraps(fn)
    def semaphore_wrapper(*args, **kwargs):
        """
    
    A decorator function that wraps another function with a threading semaphore. This decorator is used to control access to a resource by acquiring and releasing a semaphore. 
    
    Args:
        fn: The function to be wrapped.
    
    Returns:
        The wrapped function.
    
    Raises:
        Exception: If an exception occurs while executing the wrapped function.
    
    Example Usage:
        @semaphore_wrapper
        def my_function(semaphore, arg1, arg2):
            # Code logic here
    
        semaphore = threading.Semaphore()
        my_function(semaphore, arg1, arg2)
    
    """
        semaphore = kwargs['semaphore']

        logger.debug('Acquiring threading semaphore')
        semaphore.acquire()
        try:
            resp = fn(*args, **kwargs)
        except Exception:
            raise
        finally:
            semaphore.release()

        return resp

    return semaphore_wrapper


    # # # END: placeholder for supporting liquidity events
