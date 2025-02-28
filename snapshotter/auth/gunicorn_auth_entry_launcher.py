import logging
import os
import sys

from snapshotter.auth.conf import auth_settings
from snapshotter.auth.server_entry import app
from snapshotter.utils.default_logger import FORMAT
from snapshotter.utils.default_logger import logger
from snapshotter.utils.gunicorn import InterceptHandler
from snapshotter.utils.gunicorn import StandaloneApplication
from snapshotter.utils.gunicorn import StubbedGunicornLogger

JSON_LOGS = True if os.environ.get('JSON_LOGS', '0') == '1' else False
LOG_LEVEL = logging.getLevelName(os.environ.get('LOG_LEVEL', 'DEBUG'))
WORKERS = int(os.environ.get('GUNICORN_WORKERS', '5'))


if __name__ == '__main__':
    intercept_handler = InterceptHandler()
    logging.root.setLevel(LOG_LEVEL)

    seen = set()
    for name in [
        *logging.root.manager.loggerDict.keys(),
        'gunicorn',
        'gunicorn.access',
        'gunicorn.error',
        'uvicorn',
        'uvicorn.access',
        'uvicorn.error',
    ]:
        if name not in seen:
            seen.add(name.split('.')[0])
            logging.getLogger(name).handlers = [intercept_handler]

    logger.add(sys.stdout, format=FORMAT, level=LOG_LEVEL, serialize=JSON_LOGS)
    logger.add(sys.stderr, format=FORMAT, level=logging.ERROR, serialize=JSON_LOGS)

    options = {
        'bind': f'{auth_settings.bind.host}:{auth_settings.bind.port}',
        'workers': WORKERS,
        'accesslog': '-',
        'errorlog': '-',
        'worker_class': 'uvicorn.workers.UvicornWorker',
        'logger_class': StubbedGunicornLogger,
    }

    StandaloneApplication(app, options).run()
