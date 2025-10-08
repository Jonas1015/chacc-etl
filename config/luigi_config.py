import os
from decouple import Config, RepositoryEnv

env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
config = Config(RepositoryEnv(env_path))

SCHEDULER_HOST = config('LUIGI_SCHEDULER_HOST', default='localhost')
SCHEDULER_PORT = config('LUIGI_SCHEDULER_PORT', default=8082, cast=int)

WORKER_COUNT = config('LUIGI_WORKER_COUNT', default=1, cast=int)
WORKER_TIMEOUT = config('LUIGI_WORKER_TIMEOUT', default=900, cast=int)

LOG_LEVEL = config('LUIGI_LOG_LEVEL', default='INFO')
LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs', 'luigi.log')

TASK_NAMESPACE = 'etl'
MAX_RETRIES = config('MAX_RETRIES', default=3, cast=int)
RETRY_DELAY = config('RETRY_DELAY', default=60, cast=int)

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')