import signal

from snapshotter.init_rabbitmq import init_exchanges_queues
from snapshotter.process_hub_core import ProcessHubCore
from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import logger
from snapshotter.utils.exceptions import GenericExitOnSignal


def generic_exit_handler(signum, frame):
    raise GenericExitOnSignal


def main():
    for signame in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
        signal.signal(signame, generic_exit_handler)

    # setup logging
    # Using bind to pass extra parameters to the logger, will show up in the {extra} field
    launcher_logger = logger.bind(
        module='Powerloom|SnapshotterProcessHub|Core|Launcher',
        namespace=settings.namespace,
        instance_id=settings.instance_id[:5],
    )

    init_exchanges_queues()
    p_name = f'Powerloom|SnapshotterProcessHub|Core-{settings.instance_id[:5]}'
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
