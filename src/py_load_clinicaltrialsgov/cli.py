import typer
from typing_extensions import Annotated
import structlog
import logging
import sys
import json
from alembic.config import Config
from alembic import command

from py_load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from py_load_clinicaltrialsgov.connectors.interface import DatabaseConnectorInterface
from py_load_clinicaltrialsgov.extractor.api_client import APIClient
from py_load_clinicaltrialsgov.transformer.transformer import Transformer
from py_load_clinicaltrialsgov.orchestrator import Orchestrator


# Configure structlog for JSON output
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(min_level=logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
)

app = typer.Typer()
logger = structlog.get_logger(__name__)


def get_connector(name: str) -> DatabaseConnectorInterface:
    if name.lower() == "postgres":
        return PostgresConnector()
    # Add other connectors here
    logger.error("unsupported_connector", connector_name=name)
    raise ValueError(f"Unsupported connector: {name}")


@app.command()
def run(
    load_type: Annotated[
        str, typer.Option(help="Type of load: 'full' or 'delta'.")
    ] = "delta",
    connector_name: Annotated[
        str, typer.Option(help="Name of the database connector to use.")
    ] = "postgres",
) -> None:
    """
    Run the ETL process.
    """
    connector = get_connector(connector_name)
    api_client = APIClient()
    transformer = Transformer()

    orchestrator = Orchestrator(
        connector=connector, api_client=api_client, transformer=transformer
    )
    orchestrator.run_etl(load_type=load_type)


@app.command()
def init_db(
    connector_name: Annotated[
        str, typer.Option(help="Name of the database connector to use.")
    ] = "postgres",
    force: Annotated[bool, typer.Option(help="Bypass confirmation prompt.")] = False,
) -> None:
    """
    Initialize the database by dropping all existing tables and running migrations.
    Warning: This is a destructive operation.
    """
    if not force:
        confirm = typer.confirm(
            "Are you sure you want to drop all tables and re-initialize the database? "
            "This action is irreversible."
        )
        if not confirm:
            logger.warning("database_initialization_aborted")
            raise typer.Abort()

    logger.info("initializing_database")
    try:
        connector = get_connector(connector_name)
        logger.info("dropping_existing_tables")
        connector.initialize_schema()
        logger.info("tables_dropped_successfully")

        # After clearing the schema, run migrations to create it again
        migrate_db(revision="head")

    except Exception as e:
        logger.error("failed_to_initialize_database", error=str(e), exc_info=True)
        typer.echo(f"Error: Could not initialize database. {e}", err=True)
        raise typer.Exit(code=1)


@app.command()
def migrate_db(
    revision: Annotated[str, typer.Option(help="The revision to upgrade to.")] = "head"
) -> None:
    """Apply database migrations."""
    logger.info("running_database_migrations", revision=revision)
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, revision)
    logger.info("database_migrations_completed")


def _print_history(title: str, history: dict) -> None:
    """Helper function to pretty-print load history."""
    status_color = typer.colors.GREEN if history["status"] == "SUCCESS" else typer.colors.RED
    status_styled = typer.style(history["status"], fg=status_color, bold=True)

    typer.echo(typer.style(title, bold=True))
    typer.echo(f"  Timestamp: {history['load_timestamp'].isoformat()}")
    typer.echo(f"  Status: {status_styled}")
    typer.echo("  Metrics:")
    typer.echo(json.dumps(history["metrics"], indent=4))


@app.command()
def status(
    connector_name: Annotated[
        str, typer.Option(help="Name of the database connector to use.")
    ] = "postgres",
) -> None:
    """
    Check the status and history of the ETL process.
    """
    logger.info("checking_etl_status")
    try:
        connector = get_connector(connector_name)
        last_history = connector.get_last_load_history()

        if not last_history:
            typer.echo("No ETL run history found.")
            return

        # The most recent run failed
        if last_history["status"] == "FAILURE":
            header = typer.style("ETL Status: FAILED", fg=typer.colors.RED, bold=True)
            typer.echo(header)
            typer.echo(
                "The most recent ETL run failed. Details of the failure are below."
            )
            _print_history("Failed Run Details:", last_history)

            successful_history = connector.get_last_successful_load_history()
            if successful_history:
                typer.echo("-" * 20)
                typer.echo("However, a previously successful run was found.")
                _print_history(
                    "Details of Last Successful Run:", successful_history
                )
            else:
                typer.echo("No prior successful runs were found.")

        # The most recent run succeeded
        else:
            header = typer.style(
                "ETL Status: HEALTHY", fg=typer.colors.GREEN, bold=True
            )
            typer.echo(header)
            typer.echo("The most recent ETL run completed successfully.")
            _print_history("Last Run Details:", last_history)

    except Exception as e:
        logger.error("failed_to_get_status", error=str(e), exc_info=True)
        typer.echo(f"Error: Could not retrieve status. {e}", err=True)
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
