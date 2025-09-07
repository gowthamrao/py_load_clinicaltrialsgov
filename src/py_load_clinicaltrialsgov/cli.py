import typer
from typing_extensions import Annotated
import structlog
import logging
import sys

from py_load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from py_load_clinicaltrialsgov.connectors.interface import DatabaseConnectorInterface
from py_load_clinicaltrialsgov.extractor.api_client import APIClient
from py_load_clinicaltrialsgov.transformer.transformer import Transformer
from py_load_clinicaltrialsgov.config import settings


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
    load_type: Annotated[str, typer.Option(help="Type of load: 'full' or 'delta'.")] = "delta",
    connector_name: Annotated[str, typer.Option(help="Name of the database connector to use.")] = "postgres",
):
    """
    Run the ETL process.
    """
    log = logger.bind(load_type=load_type, connector_name=connector_name)
    log.info("etl_process_started")

    connector = get_connector(connector_name)
    api_client = APIClient()
    transformer = Transformer()

    try:
        connector.manage_transaction("begin")

        updated_since = None
        if load_type == "delta":
            updated_since = connector.get_last_successful_load_timestamp()
            if updated_since:
                log.info("delta_load_initiated", updated_since=updated_since.isoformat())
            else:
                log.info("no_successful_load_found_performing_full_load")

        studies_iterator = api_client.get_all_studies(updated_since=updated_since)

        record_count = 0
        for study in studies_iterator:
            nct_id = study.protocol_section.identification_module.get("nctId")
            try:
                transformer.transform_study(study)
                record_count += 1
                if record_count % 100 == 0:
                    log.info("processed_studies_batch", record_count=record_count)
            except Exception as e:
                log.error(
                    "study_transformation_failed",
                    nct_id=nct_id,
                    error=str(e),
                    exc_info=True,
                )
                if nct_id:
                    connector.record_failed_study(
                        nct_id=nct_id,
                        payload=study.model_dump(),
                        error_message=str(e),
                    )

        log.info("finished_processing_studies", total_record_count=record_count)

        dataframes = transformer.get_dataframes()
        for table_name, df in dataframes.items():
            if not df.empty:
                log.info(
                    "loading_data_into_table",
                    table_name=table_name,
                    record_count=len(df),
                )
                connector.bulk_load_staging(table_name, df)
                # The connector now handles its own primary key logic
                connector.execute_merge(table_name)

        metrics = {"records_processed": record_count}
        connector.record_load_history("SUCCESS", metrics)
        connector.manage_transaction("commit")
        log.info("etl_process_completed_successfully", metrics=metrics)

    except Exception as e:
        log.error("etl_process_failed", error=str(e), exc_info=True)
        if 'connector' in locals():
            connector.manage_transaction("rollback")
            metrics = {"error": str(e)}
            connector.record_load_history("FAILURE", metrics)
    finally:
        if 'api_client' in locals():
            api_client.close()

from alembic.config import Config
from alembic import command



@app.command()
def migrate_db(
    revision: Annotated[str, typer.Option(help="The revision to upgrade to.")] = "head"
):
    """Apply database migrations."""
    logger.info("running_database_migrations", revision=revision)
    alembic_cfg = Config("alembic.ini")
    command.upgrade(alembic_cfg, revision)
    logger.info("database_migrations_completed")


import json

@app.command()
def status(
    connector_name: Annotated[str, typer.Option(help="Name of the database connector to use.")] = "postgres",
):
    """
    Check the status of the last ETL run.
    """
    logger.info("checking_etl_status")
    try:
        connector = get_connector(connector_name)
        history = connector.get_last_load_history()

        if history:
            typer.echo("Last ETL Run Status:")
            typer.echo(f"  Timestamp: {history['load_timestamp'].isoformat()}")
            typer.echo(f"  Status: {history['status']}")
            typer.echo("  Metrics:")
            # Pretty print the JSON metrics
            typer.echo(json.dumps(history['metrics'], indent=4))
        else:
            typer.echo("No ETL run history found.")
    except Exception as e:
        logger.error("failed_to_get_status", error=str(e), exc_info=True)
        typer.echo(f"Error: Could not retrieve status. {e}", err=True)
        raise typer.Exit(code=1)

if __name__ == "__main__":
    app()
