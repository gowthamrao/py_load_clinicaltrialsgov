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
                if table_name in ["studies", "raw_studies"]:
                    primary_keys = ["nct_id"]
                else:
                    primary_keys = ["nct_id", "name"]  # Simplified for others
                connector.execute_merge(table_name, primary_keys)

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

@app.command()
def init_db(
    connector_name: Annotated[str, typer.Option(help="Name of the database connector to use.")] = "postgres",
):
    """
    Initialize the database schema.
    """
    logger.info("initializing_database_schema", connector_name=connector_name)
    connector = get_connector(connector_name)
    connector.initialize_schema()
    logger.info("database_schema_initialized")

@app.command()
def status(
    connector_name: Annotated[str, typer.Option(help="Name of the database connector to use.")] = "postgres",
):
    """
    Check the status of the last ETL run.
    """
    connector = get_connector(connector_name)
    with connector.conn.cursor() as cur:
        cur.execute("SELECT load_timestamp, status, metrics FROM load_history ORDER BY load_timestamp DESC LIMIT 1")
        result = cur.fetchone()
        if result:
            timestamp, status_val, metrics = result
            print(f"Last run at: {timestamp}")
            print(f"Status: {status_val}")
            print(f"Metrics: {metrics}")
        else:
            print("No load history found.")

if __name__ == "__main__":
    app()
