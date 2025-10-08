import os
from decouple import Config, RepositoryEnv

# Load .env file from project root
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
config = Config(RepositoryEnv(env_path))

# Source Database Configuration (where data comes from)
SOURCE_DB_HOST = config('SOURCE_DB_HOST', default='localhost')
SOURCE_DB_USER = config('SOURCE_DB_USER', default='openmrs')
SOURCE_DB_PASSWORD = config('SOURCE_DB_PASSWORD', default='Admin123')
SOURCE_DB_NAME = config('SOURCE_DB_NAME', default='openmrs')
SOURCE_DB_PORT = config('SOURCE_DB_PORT', default=3306, cast=int)

SOURCE_DB_CONFIG = {
    'host': SOURCE_DB_HOST,
    'user': SOURCE_DB_USER,
    'password': SOURCE_DB_PASSWORD,
    'database': SOURCE_DB_NAME,
    'port': SOURCE_DB_PORT,
    'charset': 'utf8mb4',
    'autocommit': True
}

# Target Database Configuration (where flattened tables go)
TARGET_DB_HOST = config('TARGET_DB_HOST', default='localhost')
TARGET_DB_USER = config('TARGET_DB_USER', default='openmrs')
TARGET_DB_PASSWORD = config('TARGET_DB_PASSWORD', default='Admin123')
TARGET_DB_NAME = config('TARGET_DB_NAME', default='icare_analytics')
TARGET_DB_PORT = config('TARGET_DB_PORT', default=3306, cast=int)

TARGET_DB_CONFIG = {
    'host': TARGET_DB_HOST,
    'user': TARGET_DB_USER,
    'password': TARGET_DB_PASSWORD,
    'database': TARGET_DB_NAME,
    'port': TARGET_DB_PORT,
    'charset': 'utf8mb4',
    'autocommit': True
}

# Connection pool settings
DB_POOL_SIZE = config('DB_POOL_SIZE', default=5, cast=int)
DB_CONNECT_TIMEOUT = config('DB_CONNECT_TIMEOUT', default=10, cast=int)