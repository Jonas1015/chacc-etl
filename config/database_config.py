import os
from decouple import Config, RepositoryEnv

env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
config = Config(RepositoryEnv(env_path))

SOURCE_DB_PROTOCOL = config('SOURCE_DB_PROTOCOL', default='mysql+pymysql')
SOURCE_DB_HOST = config('SOURCE_DB_HOST', default='localhost')
SOURCE_DB_USER = config('SOURCE_DB_USER', default='host_user')
SOURCE_DB_PASSWORD = config('SOURCE_DB_PASSWORD', default='host_password')
SOURCE_DB_NAME = config('SOURCE_DB_NAME', default='host_db')
SOURCE_DB_PORT = config('SOURCE_DB_PORT', default=3306, cast=int)

SOURCE_DB_URL = f"{SOURCE_DB_PROTOCOL}://{SOURCE_DB_USER}:{SOURCE_DB_PASSWORD}@{SOURCE_DB_HOST}:{SOURCE_DB_PORT}/{SOURCE_DB_NAME}"

SOURCE_DB_CONFIG = {
    'host': SOURCE_DB_HOST,
    'user': SOURCE_DB_USER,
    'password': SOURCE_DB_PASSWORD,
    'database': SOURCE_DB_NAME,
    'port': SOURCE_DB_PORT,
    'charset': 'utf8mb4',
    'autocommit': True
}

TARGET_DB_PROTOCOL = config('TARGET_DB_PROTOCOL', default='mysql+pymysql')
TARGET_DB_HOST = config('TARGET_DB_HOST', default='localhost')
TARGET_DB_USER = config('TARGET_DB_USER', default='target_user')
TARGET_DB_PASSWORD = config('TARGET_DB_PASSWORD', default='target_password')
TARGET_DB_NAME = config('TARGET_DB_NAME', default='target_db')
TARGET_DB_PORT = config('TARGET_DB_PORT', default=3306, cast=int)

TARGET_DB_URL = f"{TARGET_DB_PROTOCOL}://{TARGET_DB_USER}:{TARGET_DB_PASSWORD}@{TARGET_DB_HOST}:{TARGET_DB_PORT}/{TARGET_DB_NAME}"

TARGET_DB_CONFIG = {
    'host': TARGET_DB_HOST,
    'user': TARGET_DB_USER,
    'password': TARGET_DB_PASSWORD,
    'database': TARGET_DB_NAME,
    'port': TARGET_DB_PORT,
    'charset': 'utf8mb4',
    'autocommit': True
}

DB_POOL_SIZE = config('DB_POOL_SIZE', default=5, cast=int)
DB_CONNECT_TIMEOUT = config('DB_CONNECT_TIMEOUT', default=10, cast=int)