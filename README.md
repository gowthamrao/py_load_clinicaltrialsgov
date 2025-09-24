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

## Development Environment Setup

This project uses `docker-compose` to manage the PostgreSQL database and a `Makefile` to simplify common development tasks.

### Prerequisites

-   [Docker](https://docs.docker.com/get-docker/)
-   [Docker Compose](https://docs.docker.com/compose/install/)
-   A Python version manager (e.g., `pyenv`) is recommended. This project uses Python 3.11.

### 1. Set Up the Environment

First, clone the repository and navigate into the project directory:

```bash
git clone https://github.com/your-username/py-load-clinicaltrialsgov.git
cd py-load-clinicaltrialsgov
```

Next, install the project dependencies using the `Makefile`:

```bash
make install
```

This command will:
1.  Create a virtual environment using `uv`.
2.  Install all required dependencies, including development tools.

### 2. Start the Database

The development environment requires a running PostgreSQL database. You can start one easily using `docker-compose`:

```bash
make db-up
```

This command starts a PostgreSQL container in the background. The database will be available at `postgresql://user:password@localhost:5432/ctg`.

To stop the database container, run:
```bash
make db-down
```

### 3. Initialize the Database Schema

Before running the application for the first time, you need to apply the database migrations:

```bash
make init-db
```

This command executes the `alembic` migrations to create the necessary tables in the `ctg` database.

## Usage

Once the setup is complete, you can use the `Makefile` to run the ETL process or other commands.

### Run the ETL

To run a full ETL load into your local database:

```bash
make run-full
```

The `run` command from the CLI is also available, which defaults to a delta load:

```bash
# Activate the virtual environment first
source .venv/bin/activate

# Set the DSN
export DB_DSN="postgresql://user:password@localhost:5432/ctg"

# Run a delta load
py-load-clinicaltrialsgov run --connector postgres
```

### Running Tests

To run the test suite, which includes unit and integration tests:

```bash
make test
```

The integration tests require the PostgreSQL container to be running. The tests will automatically connect to the test database.

### Linting and Type Checking

To check code quality, you can run the linter and type checker:

```bash
make lint
```
