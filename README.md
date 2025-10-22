# ChaCC ETL Pipeline with Luigi: User Guide

A modern, configuration-driven ETL system built with Luigi for migrating and transforming data between databases, focusing on flattened tables and analytics capabilities.

## 🔑 Key Features

- **Configuration-Driven**: All tasks defined in JSON configuration files
- **Database Migration**: Automated creation of analytics database and tables
- **Incremental Updates**: Watermark-based tracking for efficient data updates
- **Modular Design**: Separate tasks for schema creation, data extraction, loading, and flattening
- **Error Handling**: Retry logic and structured logging
- **Dependency Management**: Automatic task ordering based on JSON-defined dependencies

## 🛠 Project Structure

```
chacc-etl/
├── config/
│   ├── database_config.py      # Database connection settings
│   └── tasks/                  # JSON task definitions
│       ├── schema.json         # Schema creation tasks
│       ├── extract_load.json   # Data extraction/loading tasks
│       ├── procedures.json     # Stored procedure tasks
│       └── flattened.json      # Data flattening tasks
├── tasks/
│   ├── base_tasks.py           # Base task classes
│   ├── schema_tasks.py         # Schema-related tasks
│   └── dynamic_task_factory_new.py  # Dynamic task creation
├── utils/
│   ├── db_utils.py            # Database utilities
│   └── logging_utils.py       # Logging utilities
├── pipelines/
│   └── main_pipeline.py       # Main pipeline orchestration
├── scripts/
│   └── run_pipeline.py        # Pipeline execution script
├── sql/
│   ├── init/                  # Database initialization scripts
│   ├── tables/                # Table creation scripts
│   ├── extract/               # Data extraction queries
│   ├── load/                  # Data loading scripts
│   └── procedures/            # Stored procedure definitions
├── luigi.cfg                  # Luigi scheduler configuration
├── requirements.txt           # Python dependencies
└── README.md                  # This file
```

## ⚙️ Installation & Setup

### Docker Deployment (Recommended)

The easiest way to run the ChaCC ETL Pipeline is using Docker with production-ready WSGI server:

```bash
# Clone the repository
git clone https://github.com/yourusername/chacc-etl.git
cd chacc-etl

# Copy environment file and configure
cp .env.example .env
# Edit .env with your database credentials

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f chacc-etl-ui
```

**Production Features:**
- **Gunicorn WSGI server** with 4 workers for high performance
- **Supervisor process manager** for reliability
- **WebSocket support** with gevent workers
- **Health checks** and automatic restarts

**Access Points:**
- **Web UI**: http://localhost:5000
- **Luigi Visualizer**: http://localhost:8082
- **MySQL**: localhost:3306
- **Redis**: localhost:6379 (optional, for multi-instance deployments)

### Manual Installation

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration

Create a `.env` file with your database credentials:

```bash
# Source Database (OpenMRS)
SOURCE_DB_HOST=localhost
SOURCE_DB_USER=your_user
SOURCE_DB_PASSWORD=your_password
SOURCE_DB_NAME=openmrs
SOURCE_DB_PORT=3306

# Target Database (Analytics)
TARGET_DB_HOST=localhost
TARGET_DB_USER=your_user
TARGET_DB_PASSWORD=your_password
TARGET_DB_NAME=icare_analytics
TARGET_DB_PORT=3306
```

### 3. Database Setup

- **Source Database**: Ensure your OpenMRS MySQL database is accessible
- **Target Database**: Create an empty MySQL database (name specified in TARGET_DB_NAME)

## 🏃 Usage & Execution

### Docker Commands

```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f chacc-etl-ui

# Rebuild and restart
docker-compose up -d --build

# Run pipeline via web UI
# Open http://localhost:5000 and click pipeline buttons
```

### Manual Execution

#### Full Migration (Initial Setup)

Creates the database, tables, and loads all data:

```bash
python scripts/run_pipeline.py --full-refresh
```

#### Incremental Migration (Updates)

Processes only new/changed data based on watermarks:

```bash
python scripts/run_pipeline.py --incremental
```

#### Custom Options

```bash
# Force complete rerun
python scripts/run_pipeline.py --full-refresh --force

# Use central scheduler
python scripts/run_pipeline.py --full-refresh --central-scheduler

# Multiple workers
python scripts/run_pipeline.py --full-refresh --workers 4
```

### Building Docker Images

For local testing and development:

```bash
# Build Docker image locally
./scripts/deploy/deploy.sh build
```

**Note**: Automated building and pushing to Docker Hub is handled by GitHub Actions workflows on releases. Users can pull and deploy the published images directly.

## 📋 Task Configuration

Tasks are defined in JSON files under `config/tasks/`. Each task specifies:

- **type**: Task category (schema, extract_load, procedure, flattened, summary)
- **dependencies**: List of prerequisite tasks
- **sql**: SQL file path or inline SQL
- **description**: Human-readable description

Example task definition:

```json
{
  "InitCreateDatabaseTask": {
    "type": "schema",
    "dependencies": [],
    "sql": "sql/init/000_create_database.sql",
    "description": "Create the analytics database"
  }
}
```

## 📈 Output Tables

### Core Tables
- `patients` - Patient demographic data
- `encounters` - Clinical encounter records
- `observations` - Clinical observations and measurements
- `locations` - Facility and location data

### Metadata Tables
- `chacc_etl_metadata` - Task execution tracking
- `chacc_etl_watermarks` - Incremental processing markers

### Flattened Tables
- `flattened_patient_encounters` - Denormalized patient-encounter data
- `flattened_observations` - Denormalized observation data

### Summary Tables
- `patient_summary` - Aggregated patient statistics
- `observation_summary` - Observation analytics
- `location_summary` - Location-based metrics

## 🐳 Docker Deployment

### Optional Redis for Multi-Instance Deployments

For single-server production deployments, Redis is not required. However, if you plan to run multiple instances of the web UI behind a load balancer for high availability:

1. **Uncomment the Redis service** in `scripts/deploy/docker-compose.prod.yml`
2. **Set the REDIS_URL environment variable** in your `.env` file:
   ```bash
   REDIS_URL=redis://redis:6379/0
   ```
3. **Uncomment redis in depends_on** and **redis_data volume** in the production compose file

This enables WebSocket message coordination across multiple app instances, ensuring all connected users see real-time pipeline progress updates.

### Production Setup

1. **Clone and configure**:
   ```bash
   git clone https://github.com/yourusername/chacc-etl-pipeline.git
   cd chacc-etl-pipeline
   cp .env.example .env
   # Edit .env with production database credentials
   ```

2. **Production deployment**:
   ```bash
   # Use production compose file
   docker-compose -f scripts/deploy/docker-compose.prod.yml up -d

   # Or use deployment script
   ./scripts/deploy/deploy.sh deploy
   ```

3. **Environment variables** for production:
   ```bash
   DOCKER_IMAGE=your-registry/chacc-etl-pipeline
   DOCKER_TAG=v1.0.0
   WEB_PORT=80
   MYSQL_PORT=3306
   REDIS_PORT=6379  # Optional: only needed for multi-instance deployments
   REDIS_URL=redis://redis:6379/0  # Optional: set to enable Redis for WebSocket coordination
   ```

### CI/CD Pipeline

The project includes GitHub Actions for automated:
- **Testing**: Runs on every push/PR
- **Building**: Creates Docker images
- **Publishing**: Pushes to Docker Hub on releases

**Required secrets** for Docker Hub publishing:
- `DOCKERHUB_USERNAME`
- `DOCKERHUB_TOKEN`

## 🐛 Troubleshooting

### Docker Issues

**Container won't start**:
```bash
docker-compose logs chacc-etl-ui
```

**Database connection issues**:
```bash
docker-compose exec mysql mysql -u root -p
```

**Permission issues**:
```bash
sudo chown -R $USER:$USER .
```

### Common Issues

**Import Errors**: Ensure virtual environment is activated and dependencies are installed.

**Database Connection**: Verify credentials in `.env` file and database accessibility.

**Task Failures**: Check logs in `logs/` directory for detailed error messages.

**Missing Dependencies**: Ensure all required JSON configuration files exist.

### Debugging

Enable debug logging:
```bash
export LUIGI_LOG_LEVEL=DEBUG
```

**Docker debug mode**:
```bash
docker-compose up  # Without -d to see logs
```

## 👩‍💻 Development

### Local Development with Docker

```bash
# Start development environment
docker-compose up -d

# View application logs
docker-compose logs -f chacc-etl-ui

# Access containers
docker-compose exec chacc-etl-ui bash

# Stop environment
docker-compose down
```

### Adding New Tasks

1. Create task definition in appropriate JSON file under `config/tasks/`
2. Add SQL file if needed under `sql/` directory
3. Define dependencies to ensure proper execution order
4. Test with `python scripts/run_pipeline.py --full-refresh`

### Task Types

- **schema**: Database and table creation
- **extract_load**: Data extraction and loading
- **procedure**: Stored procedure execution
- **flattened**: Data denormalization
- **summary**: Aggregated reporting tables

### Docker Development Workflow

```bash
# Build custom image for development
docker build -t chacc-etl-dev .

# Run with volume mounts for live code changes
docker run -v $(pwd):/app -p 5000:5000 chacc-etl-dev

# Or use docker-compose.override.yml for development
echo 'version: "3.8"
services:
  chacc-etl-ui:
    volumes:
      - .:/app
    environment:
      - FLASK_ENV=development' > docker-compose.override.yml
```

## 📜 License

This project is licensed under the **Apache License 2.0** — see the [LICENSE](./LICENSE.txt) file for details.

---

## 🚀 Quick Start with Docker

```bash
git clone https://github.com/yourusername/chacc-etl-pipeline.git
cd chacc-etl-pipeline
cp .env.example .env
# Edit .env file with your database credentials
docker-compose up -d
# Open http://localhost:5000
```

**That's it!** Your ETL pipeline with web UI is now running in Docker containers.