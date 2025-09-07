# py-load-clinicaltrialsgov

An ETL package for loading data from the ClinicalTrials.gov V2 API into a relational database.

## Overview

`py-load-clinicaltrialsgov` is a Python package designed to provide a robust and high-performance ETL solution for migrating data from the ClinicalTrials.gov V2 API into various relational database systems. It supports both full and delta loads, and is designed to be extensible to support different database backends.

## Features

- **Efficient Data Extraction**: Uses `httpx` for efficient, parallel data fetching from the ClinicalTrials.gov V2 API.
- **Robustness**: Implements retry mechanisms with exponential backoff using `tenacity` to handle transient network errors.
- **Full and Delta Loads**: Supports both full data refreshes and incremental delta loads based on the last successful run.
- **Data Validation**: Uses Pydantic models to validate the structure and types of the incoming API data.
- **Normalized Schema**: Transforms the nested JSON data into a normalized relational schema, inspired by the AACT database.
- **High-Performance Loading**: Utilizes native bulk-loading mechanisms (e.g., `COPY` for PostgreSQL) for maximum performance.
- **Extensible Architecture**: Implements a Strategy Pattern to allow for easy extension with new database connectors.
- **CLI Interface**: Provides a command-line interface for running ETL jobs and managing the database schema.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/py-load-clinicaltrialsgov.git
    cd py-load-clinicaltrialsgov
    ```

2.  **Create a virtual environment and install dependencies:**
    The package uses optional dependencies for different database backends.
    To install with PostgreSQL support, run:
    ```bash
    uv venv
    source .venv/bin/activate
    uv pip install -e .[postgres]
    ```

## Configuration

The application is configured via environment variables. The following variables are supported:

- `DB_DSN`: The Data Source Name (DSN) for connecting to your PostgreSQL database.
  - Example: `export DB_DSN="postgresql://user:password@localhost:5432/mydatabase"`
- `API_TIMEOUT`: Timeout for API requests in seconds (default: 30).
- `API_MAX_RETRIES`: Maximum number of retries for failed API requests (default: 5).
- `ETL_BATCH_SIZE`: Number of records to process in a single batch (default: 1000).
- `LOG_LEVEL`: The logging level to use (default: INFO).

You can also place these variables in a `.env` file in the project root.

## Usage

The package provides a command-line interface for operation.

### Manage the Database Schema

Before running the ETL for the first time, you need to initialize the database schema by running migrations:

```bash
py-load-clinicaltrialsgov migrate-db
```

### Run the ETL

To run the ETL process, use the `run` command:

```bash
# Run a delta load (default)
py-load-clinicaltrialsgov run --connector postgres

# Run a full load
py-load-clinicaltrialsgov run --load-type full --connector postgres
```

### Check Status

To check the status of the last ETL run, use the `status` command:

```bash
py-load-clinicaltrialsgov status --connector postgres
```

## Development

To set up the development environment and run tests:

1.  **Install development dependencies:**
    ```bash
    uv pip install -e .[dev]
    ```

2.  **Run tests:**
    ```bash
    pytest
    ```
