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
luigi-etl/
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

### 1. Environment Setup

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

### Full Migration (Initial Setup)

Creates the database, tables, and loads all data:

```bash
python scripts/run_pipeline.py --full-refresh
```

### Incremental Migration (Updates)

Processes only new/changed data based on watermarks:

```bash
python scripts/run_pipeline.py --incremental
```

### Custom Options

```bash
# Force complete rerun
python scripts/run_pipeline.py --full-refresh --force

# Use central scheduler
python scripts/run_pipeline.py --full-refresh --central-scheduler

# Multiple workers
python scripts/run_pipeline.py --full-refresh --workers 4
```

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
- `etl_metadata` - Task execution tracking
- `etl_watermarks` - Incremental processing markers

### Flattened Tables
- `flattened_patient_encounters` - Denormalized patient-encounter data
- `flattened_observations` - Denormalized observation data

### Summary Tables
- `patient_summary` - Aggregated patient statistics
- `observation_summary` - Observation analytics
- `location_summary` - Location-based metrics

## 🐛 Troubleshooting

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

## 👩‍💻 Development

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

## 📜 License

This project is licensed under the **Apache License 2.0** — see the [LICENSE](./LICENSE.txt) file for details.