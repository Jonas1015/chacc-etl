Yes, the document has already been provided in the requested Markdown format.

Here is the complete **ChaCC ETL Pipeline with Luigi: User Guide** in Markdown, including the license information:

-----

# ChaCC ETL Pipeline with Luigi: User Guide

The ChaCC ETL Pipeline is a modern, modular system built with **Luigi** for migrating and transforming data between databases. It focuses on creating **flattened tables** and providing robust **analytics capabilities**.

-----

## 🔑 Key Features

| Category | Features |
| :--- | :--- |
| **ETL & Data** | **Database Migration**, **Stored Procedures** for efficient processing, **Incremental Updates** with watermarks, **Flattened Tables** for analytics. |
| **Design** | **Modular Design** (separate tasks for Extract, Transform, Load), **Error Handling** (with retry logic), **Structured Logging**, and **Configuration** via environment variables. |
| **Interface** | **Web UI** for management, **SQL Editor** & **JSON Editor** for configurations, **File Uploads**, **Task Visualization** (via Luigi), **Dark Mode** support. |
| **Operations** | **Scheduling** (Cron-friendly updates), **Luigi Central Scheduler** support for distributed execution. |

-----

## 🛠 Project Structure

The project is organized to separate configuration, task definitions, utilities, and execution scripts for maximum clarity and maintainability.

```
chacc-etl-project/
├── config/                 # Database and Luigi configuration settings
├── tasks/                  # Luigi Task definitions (Extraction, Loading, Flattening, Schema)
├── utils/                  # Reusable code for DB connections, logging, and stored procedures
├── pipelines/              # Orchestration: Main pipeline definition
├── scripts/                # Executable runner scripts
├── data/                   # Temporary data storage
├── logs/                   # Log files
├── luigi.cfg               # Luigi scheduler configuration
├── requirements.txt        # Python dependencies
└── .env.example            # Environment variables template
```

-----

## ⚙️ Installation & Setup

Follow these steps to get the ChaCC ETL Pipeline running on your system.

### 1\. Clone & Environment Setup

```bash
# Navigate to your preferred project directory
cd /path/to/your/project

# Create a virtual environment
python -m venv .venv

# Activate the virtual environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### 2\. Install Dependencies

```bash
pip install -r requirements.txt
```

> **Troubleshooting Note**: If you encounter `pkg_resources` errors, run:
> `pip install --upgrade setuptools`

### 3\. Configure Environment Variables

Create your `.env` file from the example and fill in your database credentials and other settings.

```bash
cp .env.example .env
# Now, edit .env with your specific database and Luigi settings.
```

### 4\. Database Setup

  * **Source Database**: Ensure your Source MySQL/MariaDB database is running and accessible.
  * **Target Database (Analytics)**: Create an empty database on your target server. The pipeline will automatically create all necessary tables, stored procedures, and metadata.

-----

## ⚙️ Configuration Details

### Environment Variables (`.env` file)

The `.env` file holds all sensitive connection details and operational parameters:

  * **Source Database**: Connection details (`HOST`, `USER`, `PASSWORD`, `DATABASE`)
  * **Target Database**: Connection details for the analytics database
  * **Luigi Settings**: Scheduler, worker, and logging configurations
  * **Task Settings**: Retry logic and error handling parameters

### Luigi Scheduler Setup

For development, the pipeline uses the **local scheduler** by default. For production and the Web UI's full features, the **central scheduler** is required.

#### Starting the Central Scheduler

1.  **Uncomment** the scheduler settings in `luigi.cfg` to point to the central host/port:

    ```ini
    [core]
    default-scheduler-host=localhost
    default-scheduler-port=8082
    ```

2.  **Start the scheduler daemon**:

    ```bash
    # Ensure logs directory exists
    mkdir -p logs

    # Start the central scheduler in the background
    luigid --background --logdir logs/
    ```

3.  **Monitor**: Visit `http://localhost:8082` to view the Luigi Task Visualization web interface.

-----

## 🏃 Usage & Execution

Use the `run_pipeline.py` script to execute the ETL process.

### Full Migration (Initial Setup)

Wipes existing target data and reloads everything.

```bash
python scripts/run_pipeline.py --full-refresh
```

### Incremental Migration (Daily Updates)

Runs updates based on the last recorded watermark.

```bash
python scripts/run_pipeline.py --incremental
```

### Custom Incremental Update

Run an incremental update starting from a specific date.

```bash
python scripts/run_pipeline.py --incremental --last-updated 2024-01-01
```

### Scheduled Migration (For Cron Jobs)

A dedicated mode for scheduled runs.

```bash
python scripts/run_pipeline.py --scheduled
```

### Advanced Options

| Flag | Purpose | Example |
| :--- | :--- | :--- |
| `--central-scheduler` | Connects to the running `luigid` daemon for distributed execution. | `--central-scheduler --workers 4` |
| `--workers N` | Specifies the number of concurrent workers. | `--workers 4` |
| `--force` | Forces a complete rerun of all tasks, ignoring existing target files/flags. | `--full-refresh --force` |

-----

## 💻 Web User Interface

The Web UI provides a clean, professional interface for managing and running the pipelines.

### Starting the Web UI

```bash
python web_ui.py
```

Open your browser to **`http://localhost:5000`**.

### Web UI Features

  * **Interactive Buttons**: Run full migration, incremental, or scheduled modes with a single click.
  * **Real-time Status**: View execution results and pipeline progress.
  * **Luigi Visualizer Link**: Direct link to the task graph (requires central scheduler).
  * **SQL/JSON Editors**: Built-in editors for managing query files and task configuration files.
  * **File Upload**: Easily upload new SQL or JSON files for your pipeline.
  * **Dark/Light Mode**: Toggle for comfortable viewing.

-----

## 📈 Output Tables

The pipeline creates a balanced set of normalized and denormalized tables in your target analytics database.

### Core Tables (Normalized)

These store the clean, relational data and tracking metadata:

  * `patients`, `encounters`, `observations`, `locations`
  * `etl_metadata`, `etl_watermarks` (for pipeline tracking)

### Flattened Tables (Denormalized & Summary)

Optimized for analytics and reporting:

  * `flattened_patient_encounters`, `flattened_observations`
  * `patient_summary`, `observation_summary`, `location_summary` (Aggregated reports)

-----

## 🐛 Troubleshooting

### Common Connection Issues

| Issue | Solution |
| :--- | :--- |
| **Source DB Connection Failed** | Check `SOURCE_DB_*` settings in `.env`. Ensure the database is running and network accessible. |
| **Target DB Connection Failed** | Check `TARGET_DB_*` settings in `.env`. Ensure the analytics database exists and user has permissions. |
| **Stored Procedure Errors** | Confirm the target database user has `CREATE ROUTINE` permissions and check MySQL version compatibility. |

### Debugging

For more detailed logs, set the environment variable:
`LUIGI_LOG_LEVEL=DEBUG` in your `.env` file.

-----

## 👩‍💻 Development

### Adding New Tasks

1.  In the `tasks/` directory, create a new task class inheriting from a base class like `BaseETLTask`.
2.  Define the three core Luigi methods: `requires()`, `output()`, and `run()`.
3.  Integrate the new task into the pipeline definition in `pipelines/main_pipeline.py`.

### Scheduling with Cron

To automate daily incremental updates (e.g., at 2 AM):

```bash
# Daily incremental migration at 2 AM
0 2 * * * cd /path/to/project && python scripts/run_pipeline.py --scheduled
```

-----

## 🔮 Upcoming Features

The project is continually evolving with plans for:

  * **PostgreSQL Support**
  * **Advanced Analytics** (Data quality checks and profiling)
  * **API Endpoints** (REST API for programmatic execution)
  * **Monitoring Dashboard** (Real-time alerting)
  * **Multi-Source Support** (Heterogeneous data source extraction)

-----

## 📜 License

This project is licensed under the **Apache License 2.0** — see the [LICENSE](./LICENSE) file for details.