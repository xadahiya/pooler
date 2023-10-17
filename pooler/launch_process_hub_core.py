import signal

from setproctitle import setproctitle

from pooler.init_rabbitmq import init_exchanges_queues
from pooler.process_hub_core import ProcessHubCore
from pooler.settings.config import settings
from pooler.utils.default_logger import logger
from pooler.utils.exceptions import GenericExitOnSignal


def generic_exit_handler(signum, frame):
    """
    This function is a generic exit handler that is used to handle signals and raise a `GenericExitOnSignal` exception. It takes two parameters: `signum` and `frame`.

    Parameters:
    - `signum`: The signal number that triggered the exit handler.
    - `frame`: The current stack frame.

    This function is typically used to handle signals in a program and perform any necessary cleanup or termination actions before exiting. When a signal is received, this function raises a `GenericExitOnSignal` exception, which can be caught and handled by the calling code.

    Note: This function assumes that the `GenericExitOnSignal` exception is defined elsewhere in the code.
    """
    raise GenericExitOnSignal


def main():
    """
    This function is the main entry point of the program. It sets up signal handlers for interrupt, termination, and quit signals. It also sets the process title to a specific format using the settings namespace and instance ID.

    The function initializes the logger with specific module, namespace, and instance ID values. It then initializes the exchanges queues and creates a ProcessHubCore object with a specific name. The core process is started and its PID is logged.

    The function then waits for the core process to join. If a GenericExitOnSignal exception is raised, it logs a message indicating that the launcher received a SIGTERM signal and will attempt to join with the core process.

    Finally, the function waits for the core process to join again and logs the result. If any exception occurs during this process, it is caught and logged.

    This function is responsible for launching and managing the core process of the program.
    """
    for signame in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
        signal.signal(signame, generic_exit_handler)
    setproctitle(
        f'PowerLoom|UniswapPoolerProcessHub|Core|Launcher:{settings.namespace}-{settings.instance_id[:5]}',
    )

    # setup logging
    # Using bind to pass extra parameters to the logger, will show up in the {extra} field
    launcher_logger = logger.bind(
        module='PowerLoom|UniswapPoolerProcessHub|Core|Launcher',
        namespace=settings.namespace,
        instance_id=settings.instance_id[:5],
    )

    init_exchanges_queues()
    p_name = f'PowerLoom|UniswapPoolerProcessHub|Core-{settings.instance_id[:5]}'
    core = ProcessHubCore(name=p_name)
    core.start()
    launcher_logger.debug('Launched {} with PID {}', p_name, core.pid)
    try:
        launcher_logger.debug(
            '{} Launcher still waiting on core to join...',
            p_name,
        )
        core.join()


except GenericExitOnSignal:
        launcher_logger.debug(
            (
                '{} Launcher received SIGTERM. Will attempt to join with'
                ' ProcessHubCore process...'
            ),
            p_name,
        )
    finally:
        try:
            launcher_logger.debug(
                '{} Launcher still waiting on core to join...',
                p_name,
            )
            core.join()
        except Exception as e:
            launcher_logger.info(
                ('{} Launcher caught exception still waiting on core to' ' join... {}'),
                p_name,
                e,
            )
        launcher_logger.debug(
            '{} Launcher found alive status of core: {}',
            p_name,
            core.is_alive(),
        )


if __name__ == '__main__':
    main()
