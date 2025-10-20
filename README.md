# Database ETL Pipeline with Luigi

A comprehensive ETL (Extract, Transform, Load) pipeline built with Luigi for migrating data between databases with flattened tables and analytics.

## Project Structure

```
luigi-etl-project/
├── config/                 # Configuration files
│   ├── __init__.py
│   ├── database_config.py  # Source and target database settings
│   └── luigi_config.py     # Luigi scheduler settings
├── tasks/                  # Luigi task definitions
│   ├── __init__.py
│   ├── base_tasks.py       # Base task classes
│   ├── schema_tasks.py     # Database schema creation tasks
│   ├── extract_tasks.py    # Data extraction from source DB
│   ├── load_tasks.py       # Data loading to target DB
│   └── flattened_table_tasks.py  # Flattened table creation tasks
├── utils/                  # Utility functions
│   ├── __init__.py
│   ├── db_utils.py         # Database utilities & connection pooling
│   ├── stored_procedures.py # Stored procedures for efficient processing
│   └── logging_utils.py    # Logging configuration
├── pipelines/              # Pipeline orchestrations
│   ├── __init__.py
│   └── main_pipeline.py    # Main database migration pipeline
├── scripts/                # Executable scripts
│   └── run_pipeline.py     # Pipeline runner script
├── data/                   # Temporary data files
├── logs/                   # Log files
├── requirements.txt        # Python dependencies
├── luigi.cfg              # Luigi configuration
├── .env.example           # Environment variables template
└── README.md              # This file
```

## Features

- **Database-to-Database Migration**: Migrate data between databases with configurable schemas
- **Stored Procedures**: Efficient data processing using database stored procedures
- **Incremental Updates**: Support for incremental data updates with watermarks
- **Flattened Tables**: Automatic creation of denormalized tables for analytics
- **Modular Design**: Separate tasks for schema creation, extraction, loading, and flattening
- **Error Handling**: Retry logic and comprehensive error handling
- **Logging**: Structured logging with configurable levels
- **Configuration**: Environment-based configuration management
- **Scheduling**: Cron-friendly incremental updates
- **Web UI**: Graphical interface for running pipelines
- **SQL Editor**: Built-in SQL query editor with folder organization for managing database scripts
- **JSON Editor**: Configuration editor for pipeline task definitions with folder support
- **File Upload**: Upload SQL and JSON files with custom naming and folder organization
- **Task Visualization**: Integration with Luigi's web-based task visualizer

## Upcoming Features

- **PostgreSQL Support**: Extend database compatibility beyond MySQL/MariaDB
- **Advanced Analytics**: Built-in data quality checks and profiling
- **Cloud Integration**: Support for cloud databases (AWS RDS, Google Cloud SQL, Azure Database)
- **API Endpoints**: REST API for programmatic pipeline execution
- **Monitoring Dashboard**: Real-time pipeline monitoring and alerting
- **Data Validation**: Automated data validation and reconciliation
- **Multi-Source Support**: Extract from multiple heterogeneous data sources
- **Performance Optimization**: Query optimization and parallel processing improvements

## Installation

1. **Clone or set up the project**:
   ```bash
   cd /path/to/your/project
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

   **Note**: If you encounter `pkg_resources` errors, reinstall setuptools:
   ```bash
   pip install --upgrade setuptools
   ```

4. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and configure:

```bash
# Source Database Configuration (OpenMRS database)
SOURCE_DB_HOST=localhost
SOURCE_DB_USER=openmrs
SOURCE_DB_PASSWORD=your_openmrs_password
SOURCE_DB_NAME=openmrs

# Target Database Configuration (Analytics database)
TARGET_DB_HOST=localhost
TARGET_DB_USER=analytics
TARGET_DB_PASSWORD=your_analytics_password
TARGET_DB_NAME=icare_analytics
```

### Database Setup

1. **Source Database**: Ensure your OpenMRS MySQL/MariaDB database is running and accessible
2. **Target Database**: Create an empty analytics database (the pipeline will create all tables)

### Scheduler Setup

The pipeline uses **local scheduler** by default for development/testing. For production deployments:

1. **Uncomment** the scheduler settings in `luigi.cfg`:
   ```ini
   [core]
   default-scheduler-host=localhost
   default-scheduler-port=8082
   ```

2. **Start the central scheduler**:
   ```bash
   # Ensure logs directory exists
   mkdir -p logs

   # Start the scheduler
   luigid --background --logdir logs/
   ```

3. **Run pipelines** with `--central-scheduler` flag for distributed execution

## Usage

### Full Migration (Initial Setup)

```bash
python scripts/run_pipeline.py --full-refresh
```

### Incremental Migration (Daily Updates)

```bash
python scripts/run_pipeline.py --incremental
```

### Scheduled Incremental Migration (Cron Jobs)

```bash
python scripts/run_pipeline.py --scheduled
```

### Custom Incremental with Specific Date

```bash
python scripts/run_pipeline.py --incremental --last-updated 2024-01-01
```

### Advanced Options

```bash
# Use central scheduler (requires luigid daemon running)
python scripts/run_pipeline.py \
  --incremental \
  --central-scheduler \
  --workers 4

# Force local scheduler (default behavior)
python scripts/run_pipeline.py \
  --incremental \
  --workers 4

# Force rerun all tasks
python scripts/run_pipeline.py \
  --full-refresh \
  --force
```

## Web UI

For a graphical interface with modern styling, run the web UI:

```bash
python web_ui.py
```

Then open http://localhost:5000 in your browser. The web interface provides:
- **Interactive buttons** for all pipeline modes
- **Real-time status** display with execution results
- **Luigi Visualizer link** to monitor task graphs (requires central scheduler)
- **SQL Editor** for managing database query files with folder organization
- **JSON Editor** for configuring pipeline tasks with folder support
- **File Upload** for adding new SQL and JSON files with custom naming and folder creation
- **Clean, professional UI** with responsive design

Features:
- Full Refresh
- Incremental Update
- Scheduled Run
- Force Full Refresh
- Luigi Task Visualizer (links to http://localhost:8082)
- SQL Query Editor
- JSON Configuration Editor
- File Upload System

## Pipeline Flow

1. **Schema Creation Phase**:
   - Create target database tables
   - Create stored procedures for efficient processing
   - Initialize ETL metadata tracking

2. **Extract Phase**:
   - Extract patient data from OpenMRS database
   - Extract encounter data with full context
   - Extract observation data with concept details
   - Extract location/facility information

3. **Load Phase**:
   - Load patients into analytics database (incremental or full)
   - Load encounters with provider and location details
   - Load observations with efficient bulk operations
   - Load location data with hierarchy information

4. **Flattened Tables Phase**:
   - Create `flattened_patient_encounters` table using stored procedures
   - Create `flattened_observations` table with all related data
   - Generate patient summary statistics
   - Generate observation summary analytics
   - Create location/facility summary reports

## Output Tables

### Core Tables
- `patients`: Patient demographic and registration data
- `encounters`: Encounter records with provider and location details
- `observations`: Observation data with concept and value information
- `locations`: Facility and location hierarchy data
- `etl_metadata`: Pipeline execution tracking and statistics
- `etl_watermarks`: Incremental update tracking

### Flattened Tables
- `flattened_patient_encounters`: Patients joined with their encounters and providers
- `flattened_observations`: Observations with full patient, encounter, and concept context
- `patient_summary`: Aggregated patient statistics and visit patterns
- `observation_summary`: Concept-based observation analytics and distributions
- `location_summary`: Facility utilization and patient service statistics

## Logging

Logs are written to `logs/luigi.log` with the following levels:
- INFO: General pipeline progress
- WARNING: Non-critical issues
- ERROR: Failures and exceptions

## Monitoring

### Luigi Central Scheduler

For production use, run the Luigi central scheduler:

```bash
luigid --background --logdir logs/
```

Then run pipelines without `--local-scheduler`.

### Task Visualization

Visit `http://localhost:8082` to see the task graph and monitor progress.

## Development

### Adding New Tasks

1. Create task class inheriting from `BaseETLTask`, `SourceDatabaseTask`, or `TargetDatabaseTask`
2. Define `requires()`, `output()`, and `run()` methods
3. Add to `tasks/__init__.py`
4. Update pipeline in `pipelines/main_pipeline.py`

### Testing

```bash
# Run specific task
python -c "from tasks.extract_tasks import ExtractPatientsTask; print(ExtractPatientsTask().run())"
```

### Scheduling

Add to crontab for daily incremental updates:

```bash
# Daily incremental migration at 2 AM
0 2 * * * cd /path/to/project && python scripts/run_pipeline.py --scheduled
```

## Troubleshooting

### Common Issues

1. **Source Database Connection Failed**:
   - Check SOURCE_DB_HOST, SOURCE_DB_USER, SOURCE_DB_PASSWORD in .env
   - Ensure OpenMRS database is running and accessible

2. **Target Database Connection Failed**:
   - Check TARGET_DB_HOST, TARGET_DB_USER, TARGET_DB_PASSWORD in .env
   - Ensure analytics database exists and user has permissions

3. **Stored Procedure Errors**:
   - Ensure target database user has CREATE ROUTINE permissions
   - Check MySQL version compatibility

4. **Permission Errors**:
   - Ensure write permissions for data/, logs/ directories
   - Ensure database user has SELECT on source and CREATE/INSERT on target

### Debug Mode

Set `LUIGI_LOG_LEVEL=DEBUG` in .env for verbose logging.

## Contributing

1. Follow the existing code structure
2. Add appropriate logging
3. Include docstrings for new functions/classes
4. Test your changes

## License

This project is licensed under the Apache License 2.0 — see the [LICENSE](./LICENSE) file for details.
