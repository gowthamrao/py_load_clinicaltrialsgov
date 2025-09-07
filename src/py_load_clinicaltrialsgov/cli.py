import typer
from typing_extensions import Annotated
from py_load_clinicaltrialsgov.connectors.postgres import PostgresConnector
from py_load_clinicaltrialsgov.connectors.interface import DatabaseConnectorInterface
from py_load_clinicaltrialsgov.extractor.api_client import APIClient
from py_load_clinicaltrialsgov.transformer.transformer import Transformer
import logging

app = typer.Typer()

def get_connector(name: str) -> DatabaseConnectorInterface:
    if name.lower() == "postgres":
        return PostgresConnector()
    # Add other connectors here
    raise ValueError(f"Unsupported connector: {name}")

@app.command()
def run(
    load_type: Annotated[str, typer.Option(help="Type of load: 'full' or 'delta'.")] = "delta",
    connector_name: Annotated[str, typer.Option(help="Name of the database connector to use.")] = "postgres",
):
    """
    Run the ETL process.
    """
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    connector = get_connector(connector_name)
    api_client = APIClient()
    transformer = Transformer()

    try:
        logger.info("Starting ETL process...")
        connector.manage_transaction("begin")

        updated_since = None
        if load_type == "delta":
            updated_since = connector.get_last_successful_load_timestamp()
            if updated_since:
                logger.info(f"Performing delta load for studies updated since {updated_since}")
            else:
                logger.info("No previous successful load found, performing a full load.")

        studies_iterator = api_client.get_all_studies(updated_since=updated_since)

        record_count = 0
        for study in studies_iterator:
            try:
                transformer.transform_study(study)
                record_count += 1
                if record_count % 100 == 0:
                    logger.info(f"Processed {record_count} studies...")
            except Exception as e:
                logger.error(f"Error transforming study {study.protocol_section.identification_module.get('nctId')}: {e}")

        logger.info(f"Finished processing {record_count} studies.")

        dataframes = transformer.get_dataframes()
        for table_name, df in dataframes.items():
            if not df.empty:
                logger.info(f"Loading {len(df)} records into {table_name}...")
                if table_name == "raw_studies":
                    # For raw_studies, we can't use staging since it has a PK
                    # A more robust solution would handle this better
                    continue
                connector.bulk_load_staging(table_name, df)
                primary_keys = ["nct_id"] if table_name == "studies" else ["nct_id", "name"] # Simplified
                connector.execute_merge(table_name, primary_keys)

        metrics = {"records_processed": record_count}
        connector.record_load_history("SUCCESS", metrics)
        connector.manage_transaction("commit")
        logger.info("ETL process completed successfully.")

    except Exception as e:
        logger.error(f"ETL process failed: {e}")
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
    connector = get_connector(connector_name)
    connector.initialize_schema()
    print("Database schema initialized.")

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
            timestamp, status, metrics = result
            print(f"Last run at: {timestamp}")
            print(f"Status: {status}")
            print(f"Metrics: {metrics}")
        else:
            print("No load history found.")

if __name__ == "__main__":
    app()
